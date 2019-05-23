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
  
  override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    import loamstream.util.Observables.Implicits._
    
    val jobOracle = new JobOracle.ForJobs(executionConfig, executable.allJobs)
    
    val ioScheduler: Scheduler = IOScheduler()
    
    //An Observable stream of job runs; each job is emitted when it becomes runnable.  This can be because the
    //job's dependencies finished successfully, or because the job failed and we've decided to restart it.
    //
    //De-dupe jobs based on their ids and run counts.  This allows for re-running failed jobs, since while the
    //job id would stay the same in that case, the run count would differ.  This is a blunt-force method that 
    //prevents running the same job more than once concurrently in the face of "diamond-shaped" topologies.
    val runnables: Observable[JobRun] = RxExecuter.deDupe(executable.multiplex(_.runnables))
    
    //An observable stream of "chunks" of runnable jobs, with each chunk represented as a Seq.
    //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
    //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
    val chunks: Observable[Seq[JobRun]] = runnables.tumblingBuffer(windowLength, runner.maxNumJobs, ioScheduler)
    
    val chunkResults: Observable[Map[LJob, Execution]] = for {
      //NB: .toSet is important: jobs in a chunk should be distinct, 
      //so they're not run more than once before transitioning to a terminal state.
      jobs <- chunks.map(_.toSet)
      //NB: Filter out jobs from this chunk that finished when run as part of another chunk, so we don't run them
      //more times than necessary.  This helps in the face of job-restarting, since we can't call `distinct()` 
      //on `runnables` and declare victory like we did before, since that would filter out restarting jobs that 
      //already ran 
      (finishedJobs, notFinishedJobs) = jobs.partition(_.status.isTerminal)
      if notFinishedJobs.nonEmpty
      (jobsToMaybeRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
      (jobsToCancel, jobsToRun) = jobsToMaybeRun.partition(jobCanceler.shouldCancel)
      _ = handleSkippedJobs(skippedJobs)
      cancelledJobsMap = cancelJobs(jobsToCancel)
      _ = record(cancelledJobsMap)
      skippedResultMap = toSkippedResultMap(skippedJobs)
      executionTupleOpt <- runJobs(jobsToRun, jobOracle)
      _ = record(executionTupleOpt)
      executionMap = cancelledJobsMap ++ executionTupleOpt
      _ = logFinishedJobs(executionMap)
    } yield {
      executionMap ++ skippedResultMap
    }
    //NB: We no longer stop on the first failure, but run each sub-tree of jobs as far as possible.
    
    val z: Map[LJob, Execution] = Map.empty
    
    val futureMergedResults = chunkResults.foldLeft(z)(_ ++ _).firstAsFuture

    Await.result(futureMergedResults, timeout)
  }
  
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
    import JobStatus.FailedPermanently
    
    jobsToCancel.foreach(_.transitionTo(FailedPermanently))
    
    import loamstream.util.Traversables.Implicits._
    
    jobsToCancel.mapTo(job => Execution.from(job, FailedPermanently, terminationReason = None))
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

  private def record(executionTuples: Iterable[(LJob, Execution)]): Unit = executionRecorder.record(executionTuples)
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
          
          job.transitionTo(finalStatus)
        }
      }
    }
    
    Observable.from(jobToExecutionFutures).flatMap(Observable.from(_))
  }
}
