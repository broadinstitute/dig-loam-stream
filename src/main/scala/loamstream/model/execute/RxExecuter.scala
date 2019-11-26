package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationDouble

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobRun
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.Maps
import loamstream.util.Observables
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import loamstream.model.jobs.JobNode
import loamstream.conf.ExecutionConfig
import loamstream.util.ValueBox
import java.io.FileWriter
import loamstream.util.ExecutionContexts
import loamstream.util.Terminable
import java.io.PrintWriter
import java.time.Instant
import loamstream.model.jobs.log.JobLog
import loamstream.model.jobs.RunData
import scala.concurrent.Future
import loamstream.conf.LoamConfig
import loamstream.util.FileMonitor
import loamstream.util.Futures
import jdk.nashorn.internal.runtime.FinalScriptFunctionData
import loamstream.model.jobs.JobOracle
import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.Subscription

/**
 * @author kaan
 * @author clint
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    executionConfig: ExecutionConfig,
    runner: ChunkRunner,
    fileMonitor: FileMonitor,
    windowLength: Duration,
    jobCanceler: JobCanceler,
    jobFilter: JobFilter,
    executionRecorder: ExecutionRecorder,
    maxRunsPerJob: Int,
    override protected val terminableComponents: Iterable[Terminable] = Nil)
    (implicit val executionContext: ExecutionContext) extends Executer with Terminable.StopsComponents with Loggable {
  
  require(maxRunsPerJob >= 1, s"The maximum number of times to run each job must not be negative; got $maxRunsPerJob")
  
  import executionRecorder.record
  import loamstream.util.Observables.Implicits._
  
  override def execute(
      executable: Executable, 
      makeJobOracle: Executable => JobOracle = JobOracle.fromExecutable(executionConfig, _))
     (implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    
    val jobOracle = makeJobOracle(executable)
    
    val ioScheduler: Scheduler = IOScheduler()
    
    //An Observable stream of job runs; each job is emitted when it becomes runnable.  This can be because the
    //job's dependencies finished successfully, or because the job failed and we've decided to restart it.
    //
    //De-dupe jobs based on their ids and run counts.  This allows for re-running failed jobs, since while the
    //job id would stay the same in that case, the run count would differ.  This is a blunt-force method that 
    //prevents running the same job more than once concurrently in the face of "diamond-shaped" topologies.
    val runnables: ConnectableObservable[JobRun] = RxExecuter.deDupe(executable.multiplex(_.runnables)).publish//.share
    
    val (futureMergedResults, _) = andThenConnect(runnables) {
      //Split stream of runnable jobs into one stream for each environment type, so that they may be handled
      //differently, say by buffering.
      val byEnvironment: Map[EnvironmentType, Observable[JobRun]] = splitByEnvironment(runnables)

      //Transform streams of jobs by (optionally) buffering them.
      val bufferedRunsByEnvironment = buffer(ioScheduler)(byEnvironment)
      
      val chunkResults: Observable[Map[LJob, Execution]] = {
        //Merge all the per-environment streams of chunks of runnable jobs into one stream of chunks of runnable jobs,
        //then run the chunks 
        val allJobRuns: Observable[Seq[JobRun]] = Observables.merge(bufferedRunsByEnvironment.values)
        
        runChunks(jobOracle)(allJobRuns)
      }

      //Accumulate results of running chunks of jobs in a map; emit the accumulated map once all chunks are done.
      //NB: We no longer stop on the first failure, but run each sub-tree of jobs as far as possible.
      chunkResults.foldLeft(emptyExecutionMap)(_ ++ _).firstAsFuture
    }
    
    Await.result(futureMergedResults, timeout)
  }
  
  private def runChunks(jobOracle: JobOracle)(chunks: Observable[Seq[JobRun]]) : Observable[Map[LJob, Execution]] = {
    def isTerminal(jr: JobRun): Boolean = jr.status.isTerminal
    
    for {
      //NB: .toSet is important: jobs in a chunk should be distinct, 
      //so they're not run more than once before transitioning to a terminal state.
      jobs <- chunks.map(_.toSet)
      //NB: Filter out jobs from this chunk that finished when run as part of another chunk, so we don't run them
      //more times than necessary.  This helps in the face of job-restarting, since we can't call `distinct()` 
      //on `runnables` and declare victory like we did before, since that would filter out restarting jobs that 
      //already ran 
      (finishedJobs, notFinishedJobs) = jobs.partition(isTerminal)
      if notFinishedJobs.nonEmpty
      (jobsToMaybeRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
      (jobsToCancel, jobsToRun) = jobsToMaybeRun.partition(jobCanceler.shouldCancel)
      _ = handleSkippedJobs(skippedJobs)
      cancelledJobsMap = cancelJobs(jobsToCancel)
      _ = record(jobOracle, cancelledJobsMap)
      skippedJobsMap = toSkippedResultMap(skippedJobs)
      executedJobTupleOpt <- runJobs(jobsToRun, jobOracle)
      _ = record(jobOracle, executedJobTupleOpt)
      executedJobsMap = cancelledJobsMap ++ executedJobTupleOpt
      _ = logFinishedJobs(executedJobsMap)
    } yield {
      executedJobsMap ++ skippedJobsMap
    }
  }

  /**
   * Buffer streams of JobRuns differently for different environments.
   */
  private def buffer[A](scheduler: Scheduler)
               (byEnvironment: Map[EnvironmentType, Observable[A]]): Map[EnvironmentType, Observable[Seq[A]]] = {
    
    byEnvironment.map { 
      //Don't buffer local jobs - process them right away
      case (EnvironmentType.Local, as) => (EnvironmentType.Local, as.map(Seq(_)))
      //Otherwise, buffer up runnable jobs, for example to make sure that as many jobs as possible
      //are run in a single Uger/LSF task array, etc.
      case (et, as) => (et, as.tumblingBuffer(windowLength, runner.maxNumJobs, scheduler))
    }
  }
  
  /**
   * Split a stream of JobRuns into several streams, one for each environment type, so we can buffer JobRuns
   * differently for different environments.
   */
  private def splitByEnvironment(jobRuns: Observable[JobRun]): Map[EnvironmentType, Observable[JobRun]] = {
    import EnvironmentType._
    
    def envType(jr: JobRun): EnvironmentType = jr.job.initialSettings.envType
    
    def toPredicate(et: EnvironmentType)(jr:JobRun): Boolean = envType(jr) == et 
    
    def runsForEnv(et: EnvironmentType): Observable[JobRun] = jobRuns.filter(toPredicate(et))
    
    def tupleFor(et: EnvironmentType): (EnvironmentType, Observable[JobRun]) = et -> runsForEnv(et)
    
    Map(
      tupleFor(Local),
      tupleFor(Google),
      tupleFor(Uger),
      tupleFor(Lsf),
      tupleFor(Aws))
  }
  
  private def andThenConnect[A, B](cr: ConnectableObservable[A])(body: => B): (B, Subscription) = {
    val b = body
    val subscription = cr.connect
    
    b -> subscription
  }
  
  private val emptyExecutionMap: Map[LJob, Execution] = Map.empty
  
  private def logFinishedJobs(jobs: Map[LJob, Execution]): Unit = {
    for {
      (job, execution) <- jobs
    } {
      info(s"Finished with ${execution.status} (${execution.result}) when running $job")
    }
  }
  
  //NB: shouldRestart() mostly factored out to the companion object for simpler testing
  private def shouldRestart(job: LJob): Boolean = RxExecuter.shouldRestart(job, maxRunsPerJob)
  
  //Produce Optional LJob -> Execution tuples.  We need to be able to produce just one (empty) item,
  //instead of just returning Observable.empty, so that code chained onto this method's result with
  //flatMap will run.
  private def runJobs(jobsToRun: Iterable[LJob], jobOracle: JobOracle): Observable[Option[(LJob, Execution)]] = {
    logJobsToBeRun(jobsToRun)
    
    import RxExecuter.toExecutionMap
    
    val emptyMap = Map.empty[LJob, Execution]
    
    if(jobsToRun.isEmpty) { Observable.just(None) }
    else {
      val jobRunObs = runner.run(jobsToRun.toSet, jobOracle, shouldRestart)
      
      jobRunObs.flatMap(toExecutionMap(fileMonitor, shouldRestart)).map(Option(_))
    }
  }
  
  private def cancelJobs(jobsToCancel: Iterable[LJob]): Map[LJob, Execution] = {
    import JobStatus.Canceled
    
    jobsToCancel.foreach(_.transitionTo(Canceled))
    
    import loamstream.util.Traversables.Implicits._
    
    jobsToCancel.mapTo(job => Execution.from(job, Canceled, terminationReason = None))
  }
  
  private def handleSkippedJobs(skippedJobs: Iterable[LJob]): Unit = {
    logSkippedJobs(skippedJobs)
    
    markJobsSkipped(skippedJobs)
  }
  
  private def logJobsToBeRun(jobsToRun: Iterable[LJob]): Unit = {
    debug(s"Dispatching (${jobsToRun.size}) jobs to ChunkRunner:")
    
    jobsToRun.foreach(job => debug(s"Dispatching job to ChunkRunner: $job"))
  }
  
  private def logSkippedJobs(skippedJobs: Iterable[LJob]): Unit = skippedJobs.size match {
    case 0 => debug("Skipped 0 jobs")
    case numSkipped => {
      info(s"Skipped ($numSkipped) jobs:")
    
      skippedJobs.foreach(job => info(s"Skipped: $job"))
    }
  }
  
  private def markJobsSkipped(skippedJobs: Iterable[LJob]): Unit = {
    skippedJobs.foreach(_.transitionTo(JobStatus.Skipped))
  }
  
  private def toSkippedResultMap(skippedJobs: Iterable[LJob]): Map[LJob, Execution] = {
    import loamstream.util.Traversables.Implicits._
      
    skippedJobs.mapTo(job => Execution.from(job, JobStatus.Skipped, terminationReason = None))
  }
}

object RxExecuter extends Loggable {
  object Defaults {
    //NB: Use a short windowLength to speed up tests
    val windowLength: Double = 0.05
    val windowLengthInSec: Duration = windowLength.seconds
  
    val jobCanceler: JobCanceler = JobCanceler.NeverCancel
    
    val jobFilter: JobFilter = JobFilter.RunEverything
  
    val executionRecorder: ExecutionRecorder = ExecutionRecorder.DontRecord
    
    val maxRunsPerJob: Int = 4
    
    val executionConfig: ExecutionConfig = ExecutionConfig.default
    
    lazy val maxNumConcurrentJobs: Int = AsyncLocalChunkRunner.defaultMaxNumJobs
    
    lazy val maxWaitTimeForOutputs: Duration = executionConfig.maxWaitTimeForOutputs
    
    lazy val outputExistencePollingFrequencyInHz: Double = executionConfig.outputPollingFrequencyInHz
    
    lazy val fileMonitor: FileMonitor = new FileMonitor(outputExistencePollingFrequencyInHz, maxWaitTimeForOutputs)
  }
  
  def apply(runner: ChunkRunner)(implicit executionContext: ExecutionContext): RxExecuter = {
    new RxExecuter(
        Defaults.executionConfig,
        runner, 
        Defaults.fileMonitor, 
        Defaults.windowLengthInSec,
        Defaults.jobCanceler,
        Defaults.jobFilter, 
        Defaults.executionRecorder,
        Defaults.maxRunsPerJob)
  }

  def default: RxExecuter = {
    val (executionContext, ecHandle) = ExecutionContexts.threadPool(Defaults.maxNumConcurrentJobs)

    val chunkRunner = AsyncLocalChunkRunner(Defaults.executionConfig, Defaults.maxNumConcurrentJobs)(executionContext)

    new RxExecuter(
        Defaults.executionConfig,
        chunkRunner, 
        Defaults.fileMonitor,
        Defaults.windowLengthInSec, 
        Defaults.jobCanceler,
        Defaults.jobFilter, 
        Defaults.executionRecorder,
        Defaults.maxRunsPerJob, 
        Option(ecHandle))(executionContext)
  }
  
  def defaultWith(
      newJobFilter: JobFilter = Defaults.jobFilter, 
      newExecutionRecorder: ExecutionRecorder = Defaults.executionRecorder): RxExecuter = {
    
    def addJobFilter(rxe: RxExecuter): RxExecuter = {
      implicit val context = rxe.executionContext 
      
      if(newJobFilter eq rxe.jobFilter) rxe else rxe.copy(jobFilter = newJobFilter)
    }
    
    def addExecutionRecorder(rxe: RxExecuter): RxExecuter = {
      implicit val context = rxe.executionContext
      
      if(newExecutionRecorder eq rxe.executionRecorder) rxe else rxe.copy(executionRecorder = newExecutionRecorder)
    }
    
    addExecutionRecorder(addJobFilter(default))
  }
  
  private[execute] def deDupe(jobRuns: Observable[JobRun]): Observable[JobRun] = jobRuns.distinct(_.key)
  
  private[execute] def shouldRestart(job: LJob, maxRunsPerJob: Int): Boolean = {
    val runCount = job.runCount
    
    val result = runCount < maxRunsPerJob
    
    debug(s"Restarting $job ? $result (job has run $runCount times, max is $maxRunsPerJob)")
    
    result
  }

  /**
   * Turns the passed `runDataMap` into an observable that will fire once, producing a Map derived from `runDataMap`
   * by turning `runDataMap`'s values into Executions after waiting for any missing outputs. 
   */
  private[execute] def toExecutionMap(
      fileMonitor: FileMonitor,
      shouldRestart: LJob => Boolean)
      (runDataMap: Map[LJob, RunData])
      (implicit context: ExecutionContext): Observable[(LJob, Execution)] = {
  
    def waitForOutputs(runData: RunData): Future[Execution] = {
      ExecuterHelpers.waitForOutputsAndMakeExecution(runData, fileMonitor)
    }
    
    val jobToExecutionFutures = for {
      (job, runData) <- runDataMap.toSeq
    } yield {
      import Futures.Implicits._
      
      waitForOutputs(runData).map(execution => job -> execution).withSideEffect {
        //Transition Job to whatever its ultimate status was: 
        //  WaitingForOutputs => Succeeded | Failed
        //  foo => foo
        case (job, execution) => {
          val finalStatus = {
            if(execution.status.isFailure) {
              ExecuterHelpers.determineFailureStatus(shouldRestart, execution.status, job) 
            } else {
              execution.status
            }
          }
          
          trace(
            s"Done waiting for outputs, transitioning job $job with execution $execution " +
            s"to final status '$finalStatus'")
          
          job.transitionTo(finalStatus)
        }
      }
    }
    
    Observable.from(jobToExecutionFutures).flatMap(Observable.from(_))
  }
}
