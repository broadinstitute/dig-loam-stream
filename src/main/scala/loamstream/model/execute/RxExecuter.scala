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
import loamstream.drm.DrmChunkRunner
import loamstream.googlecloud.GoogleCloudChunkRunner

/**
 * @author kaan
 * @author clint
 *         date: Aug 17, 2016
 */
final case class RxExecuter(
    executionConfig: ExecutionConfig,
    private val makeRunner: ChunkRunner.Constructor[ChunkRunner],
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
  
  private def withRunner[A](jobOracle: JobOracle)(body: ChunkRunner => A): A = {
    val runner: ChunkRunner = makeRunner(shouldRestart, jobOracle)
    
    try { body(runner) }
    finally { /*runner.stop()*/ }
  }
  
  private def determinePlan(jobRun: JobRun): RxExecuter.JobPlan = {
    val job = jobRun.job
    val jobIsFinished = jobRun.status.isTerminal
    if(jobIsFinished) { RxExecuter.JobPlan.DontRun(job) }
    else {
      val shouldMaybeRun = jobFilter.shouldRun(job)
      val shouldSkip = !shouldMaybeRun
      if(shouldSkip) { RxExecuter.JobPlan.Skip(job) }
      else {
        val shouldCancel = jobCanceler.shouldCancel(job)
        if(shouldCancel) { RxExecuter.JobPlan.Cancel(job) }
        else { RxExecuter.JobPlan.Run(job) }
      }
    }
  }
  
  override def execute(
      executable: Executable, 
      makeJobOracle: Executable => JobOracle = JobOracle.fromExecutable(executionConfig, _))
     (implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
    
    val jobOracle = makeJobOracle(executable)

    withRunner(jobOracle) { runner =>
      //An Observable stream of job runs; each job is emitted when it becomes runnable.  This can be because the
      //job's dependencies finished successfully, or because the job failed and we've decided to restart it.
      //
      //De-dupe jobs based on their ids and run counts.  This allows for re-running failed jobs, since while the
      //job id would stay the same in that case, the run count would differ.  This is a blunt-force method that 
      //prevents running the same job more than once concurrently in the face of "diamond-shaped" topologies.
      val runnables: Observable[JobRun] = RxExecuter.deDupe(executable.multiplex(_.runnables)).subscribeOn(IOScheduler())//.share
      
      def chunkResults(chunks: Observable[Seq[JobRun]]): Observable[Map[LJob, Execution]] = {
        for {
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
          _ = record(jobOracle, cancelledJobsMap)
          skippedResultMap = toSkippedResultMap(skippedJobs)
          executionTupleOpt <- runJobs(runner, jobsToRun)
          _ = record(jobOracle, executionTupleOpt)
          executionMap = cancelledJobsMap ++ executionTupleOpt
          _ = logFinishedJobs(executionMap)
        } yield {
          executionMap ++ skippedResultMap
        }
      }
      
      import Maps.Implicits.MapOps
      
      val byEnvType = splitByEnvironmentType(runner.maxNumJobs)(runnables)
      
      val streamsForChunks = byEnvType.values.map(chunkResults)
      
      val futureMergedResults = Observables.merge(streamsForChunks).foldLeft(emptyExecutionMap)(_ ++ _).firstAsFuture
      
      /*def chunkResults(rs: Observable[Seq[JobRun]]): Observable[Map[LJob, Execution]] = for {
        jobRuns <- runnables
        jobs
        job = jobRun.job
        plan = determinePlan(jobRun)
        //NB: Filter out jobs from this chunk that finished when run as part of another chunk, so we don't run them
        //more times than necessary.  This helps in the face of job-restarting, since we can't call `distinct()` 
        //on `runnables` and declare victory like we did before, since that would filter out restarting jobs that 
        //already ran 
        notFinished = !jobRun.status.isTerminal
        if notFinished
        _ = handleSkippedJobs(plan.skippedJobOpt)
        cancelledJobsMap = cancelJobs(plan.cancelledJobOpt)
        _ = record(jobOracle, cancelledJobsMap)
        skippedResultMap = toSkippedResultMap(plan.skippedJobOpt)
        executionTupleOpt <- runJobs(runner, plan.jobToRunOpt)
        _ = record(jobOracle, executionTupleOpt)
        executionMap = cancelledJobsMap ++ executionTupleOpt
        _ = logFinishedJobs(executionMap)
      } yield {
        executionMap ++ skippedResultMap
      }*/

      //An observable stream of "chunks" of runnable jobs, with each chunk represented as a Seq.
      //Jobs are buffered up until the amount of time indicated by 'windowLength' elapses, or 'runner.maxNumJobs'
      //are collected.  When that happens, the buffered "chunk" of jobs is emitted.
      //val chunks: Observable[Seq[JobRun]] = runnables.tumblingBuffer(1.milliseconds/*windowLength*/, 2 /*runner.maxNumJobs*/, IOScheduler())
      //val chunks: Observable[Seq[JobRun]] = runnables.tumblingBuffer(windowLength, runner.maxNumJobs, IOScheduler())
      //      val chunks: Observable[Set[JobRun]] = runnables.map(Set(_))
      //
      //      val chunkResults: Observable[Map[LJob, Execution]] = for {
      //        //NB: .toSet is important: jobs in a chunk should be distinct,
      //        //so they're not run more than once before transitioning to a terminal state.
      //        jobs <- chunks.map(_.toSet)
      //        //NB: Filter out jobs from this chunk that finished when run as part of another chunk, so we don't run them
      //        //more times than necessary.  This helps in the face of job-restarting, since we can't call `distinct()`
      //        //on `runnables` and declare victory like we did before, since that would filter out restarting jobs that
      //        //already ran
      //        (finishedJobs, notFinishedJobs) = jobs.partition(_.status.isTerminal)
      //        if notFinishedJobs.nonEmpty
      //        (jobsToMaybeRun, skippedJobs) = notFinishedJobs.map(_.job).partition(jobFilter.shouldRun)
      //        (jobsToCancel, jobsToRun) = jobsToMaybeRun.partition(jobCanceler.shouldCancel)
      //        _ = handleSkippedJobs(skippedJobs)
      //        cancelledJobsMap = cancelJobs(jobsToCancel)
      //        _ = record(jobOracle, cancelledJobsMap)
      //        skippedResultMap = toSkippedResultMap(skippedJobs)
      //        executionTupleOpt <- runJobs(runner, jobsToRun)
      //        _ = record(jobOracle, executionTupleOpt)
      //        executionMap = cancelledJobsMap ++ executionTupleOpt
      //        _ = logFinishedJobs(executionMap)
      //      } yield {
      //        executionMap ++ skippedResultMap
      //      }
      //NB: We no longer stop on the first failure, but run each sub-tree of jobs as far as possible.

      //val futureMergedResults = chunkResults.foldLeft(emptyExecutionMap)(_ ++ _).firstAsFuture
  
      //runnables.last.foreach(_ => runner.stop())
      
      //runner.stop()
      
      Await.result(futureMergedResults, timeout)
    }
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
  private def runJobs(runner: ChunkRunner, jobsToRun: Iterable[LJob]): Observable[Option[(LJob, Execution)]] = {
    logJobsToBeRun(jobsToRun)
    
    import RxExecuter.toExecutionMap
    
    val emptyMap = Map.empty[LJob, Execution]
    
    if(jobsToRun.isEmpty) { Observable.just(None) }
    else {
      val jobRunObs = runner.run(jobsToRun.toSet)
      
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
  
  private def splitByEnvironmentType(bufferSize: Int)(jobRuns: Observable[JobRun]): Map[EnvironmentType, Observable[Seq[JobRun]]] = {
    def envType(jr: JobRun): EnvironmentType = jr.job.initialSettings.envType
    
    val shared = jobRuns.share
    
    import EnvironmentType._
    
    Map(
      Local -> shared.filter(_.job.initialSettings.envType.isLocal).tumblingBuffer(windowLength, bufferSize, IOScheduler()),
      Uger -> shared.filter(_.job.initialSettings.envType.isUger).map(Seq(_)),
      Lsf -> shared.filter(_.job.initialSettings.envType.isLsf).map(Seq(_)),
      Google -> shared.filter(_.job.initialSettings.envType.isGoogle).map(Seq(_)),
      Aws -> shared.filter(_.job.initialSettings.envType.isAws).map(Seq(_)))
  }
}

object RxExecuter extends Loggable {
  
  
  sealed trait EnvironmentStrategy {
    type CR <: ChunkRunner
    
    def chunkRunner(ccr: CompositeChunkRunner): Option[CR] 
  }
  
  object EnvironmentStrategy {
    case object LocalEnvironmentStrategy extends EnvironmentStrategy {
      override type CR = AsyncLocalChunkRunner
      
      override def chunkRunner(ccr: CompositeChunkRunner): Option[AsyncLocalChunkRunner] = {
        ccr.components.collectFirst { case alcr: AsyncLocalChunkRunner => alcr }
      }
    }
    
    case object UgerEnvironmentStrategy extends EnvironmentStrategy {
      override type CR = DrmChunkRunner
      
      override def chunkRunner(ccr: CompositeChunkRunner): Option[DrmChunkRunner] = {
        ccr.components.collectFirst { case dcr: DrmChunkRunner if dcr.environmentType.isUger => dcr }
      }
    }
    
    case object LsfEnvironmentStrategy extends EnvironmentStrategy {
      override type CR = DrmChunkRunner
      
      override def chunkRunner(ccr: CompositeChunkRunner): Option[DrmChunkRunner] = {
        ccr.components.collectFirst { case dcr: DrmChunkRunner if dcr.environmentType.isLsf => dcr }
      }
    }
    
    case object GoogleEnvironmentStrategy extends EnvironmentStrategy {
      override type CR = GoogleCloudChunkRunner
      
      override def chunkRunner(ccr: CompositeChunkRunner): Option[GoogleCloudChunkRunner] = {
        ccr.components.collectFirst { case dcr: GoogleCloudChunkRunner => dcr }
      }
    }
  }
  
  sealed abstract class JobPlan {
    def job: LJob
    
    final def shouldNotRun: Boolean = this.isInstanceOf[JobPlan.DontRun]
    final def shouldCancel: Boolean = this.isInstanceOf[JobPlan.Cancel]
    final def shouldSkip: Boolean = this.isInstanceOf[JobPlan.Skip]
    final def shouldRun: Boolean = this.isInstanceOf[JobPlan.Run]
    
    final def skippedJobOpt: Option[LJob] = if(shouldSkip) Some(job) else None
    final def cancelledJobOpt: Option[LJob] = if(shouldCancel) Some(job) else None
    final def jobToRunOpt: Option[LJob] = if(shouldRun) Some(job) else None
  }
  
  object JobPlan {
    final case class DontRun(job: LJob) extends JobPlan
    final case class Skip(job: LJob) extends JobPlan
    final case class Cancel(job: LJob) extends JobPlan
    final case class Run(job: LJob) extends JobPlan
  }
  
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
        (_, _) => runner, 
        Defaults.fileMonitor, 
        Defaults.windowLengthInSec,
        Defaults.jobCanceler,
        Defaults.jobFilter, 
        Defaults.executionRecorder,
        Defaults.maxRunsPerJob)
  }

  def default: RxExecuter = {
    val (executionContext, ecHandle) = ExecutionContexts.threadPool(Defaults.maxNumConcurrentJobs)

    val makeChunkRunner: ChunkRunner.Constructor[ChunkRunner] = { (shouldRestart, jobOracle) =>
      AsyncLocalChunkRunner(
          executionConfig = Defaults.executionConfig, 
          jobOracle = jobOracle, 
          shouldRestart = shouldRestart, 
          maxNumJobs = Defaults.maxNumConcurrentJobs)(executionContext)
    }

    new RxExecuter(
        Defaults.executionConfig,
        makeChunkRunner, 
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
