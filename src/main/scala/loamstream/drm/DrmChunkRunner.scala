package loamstream.drm

import DrmStatus.toJobResult
import DrmStatus.toJobStatus
import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.jobs.DrmJobOracle
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobStatus.Failed
import loamstream.model.jobs.JobStatus.Running
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.Classes.simpleNameOf
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import monix.execution.Scheduler
import monix.reactive.Observable
import loamstream.util.TimeUtils
import loamstream.util.ExitCodes
import cats.kernel.Eq
import loamstream.model.jobs.commandline.HasCommandLine
import scala.collection.compat._
import loamstream.util.Maps
import monix.eval.Task

/**
 * @author clint
 *         date: Jul 1, 2016
 *
 *         A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided JobSubmitter.
 */
final case class DrmChunkRunner(
    environmentType: EnvironmentType,
    pathBuilder: PathBuilder,
    executionConfig: ExecutionConfig,
    drmConfig: DrmConfig,
    jobSubmitter: JobSubmitter,
    jobMonitor: JobMonitor,
    accountingClient: AccountingClient,
    jobKiller: JobKiller,
    private val sessionTracker: SessionTracker
  ) extends ChunkRunnerFor(environmentType) with 
        Terminable.StopsComponents with Loggable {

  require(environmentType.isUger || environmentType.isLsf, "Only UGER and LSF environments are supported")
  
  import DrmChunkRunner._

  override protected val terminableComponents: Iterable[Terminable] = {
    val jobKillerTerminable: Terminable = Terminable(jobKiller.killAllJobs())
    
    Seq(jobKillerTerminable, jobSubmitter, jobMonitor)
  }

  def submittedTaskArrayIds: Iterable[String] = sessionTracker.taskArrayIdsSoFar
  
  /**
   * Run the provided jobs, using the provided predicate (`shouldRestart`) to decide whether to re-run them if they
   * fail.  Returns an Observable producing a map of jobs to Executions.
   *
   * NB: NoOpJobs are ignored.  Otherwise, this method expects that all the other jobs are CommandLineJobs, and
   * will throw otherwise.
   */
  override def run(
      jobs: Iterable[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {

    debug(s"${getClass.getSimpleName}: Running ${jobs.size} jobs: ")
    jobs.foreach(job => debug(s"  $job"))

    require(
      jobs.forall(_.isInstanceOf[CommandLineJob]),
      s"For now, we only know how to run ${simpleNameOf[CommandLineJob]}s on a DRM system")

    // Filter out NoOpJob's
    val commandLineJobs = jobs.to(Seq).collect { case clj: CommandLineJob => clj }

    //Group Jobs by their uger settings, and run each group.  This is necessary because the jobs in a group will
    //be run as 1 Uger task array, and Uger params are per-task-array.
    val resultsForSubChunks: Iterable[Observable[(LJob, RunData)]] = {
      import DrmTaskArray.fromCommandLineJobs
      
      val maxJobsPerTaskArray = drmConfig.maxNumJobsPerTaskArray
      
      for {
        (settings, rawJobs) <- subChunksBySettings(commandLineJobs)
        rawJobChunk <- rawJobs.sliding(maxJobsPerTaskArray, maxJobsPerTaskArray)
      } yield {
        val drmTaskArray = TimeUtils.time(s"Making DrmTaskArray with ${rawJobChunk.size} jobs", debug(_)) {
          fromCommandLineJobs(executionConfig, jobOracle, settings, drmConfig, pathBuilder, rawJobChunk)
        }
        
        runJobs(settings, drmTaskArray, jobOracle)
      }
    }

    if (resultsForSubChunks.isEmpty) { Observable.empty }
    else { Observables.merge(resultsForSubChunks) }
  }

  private def onSubmit(submissionResult: DrmSubmissionResult): DrmSubmissionResult = {
    submissionResult.foreach(drmJobsByDrmId => sessionTracker.register(drmJobsByDrmId.keys))
    
    submissionResult
  }
  
  private[drm] def submit(drmSettings: DrmSettings, drmTaskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
    import drmTaskArray.{ drmJobName => name }
    
    val msg = s"Submitting task array ${name} with ${drmTaskArray.size} jobs"
    
    TimeUtils.time(msg, debug(_)) {
      debug(msg)
      
      import loamstream.drm.DrmSubmissionResult._
      
      jobSubmitter.submitJobs(drmSettings, drmTaskArray).map(onSubmit)
    }
  }

  /**
   * Submits the provided CommandLineJobs and monitors them, resulting in an Observable producing a map of jobs to
   * Executions
   */
  private def runJobs(
    drmSettings: DrmSettings,
    drmTaskArray: DrmTaskArray,
    jobOracle: JobOracle): Observable[(LJob, RunData)] = {

    drmTaskArray.drmJobs match {
      case Nil => Observable.empty
      case drmJobs => submit(drmSettings, drmTaskArray).flatMap(toRunDataStream(jobOracle, drmJobs))
    }
  }
  
  private def toRunDataStream
    (jobOracle: JobOracle,
     drmJobs: Seq[DrmJobWrapper])(
    submissionResult: DrmSubmissionResult): Observable[(LJob, RunData)] = {
    
    val commandLineJobs = drmJobs.map(_.commandLineJob)
    
    import loamstream.drm.DrmSubmissionResult._
    
    submissionResult match {
      case SubmissionSuccess(drmJobsByDrmId) => jobsToRunDatas(drmJobsByDrmId, jobOracle)
      case SubmissionFailure(e) => makeAllFailureMap(drmJobs, Some(e))
    }
  }
  
  private[drm] def jobsToRunDatas(
    jobsById: Map[DrmTaskId, DrmJobWrapper],
    jobOracle: JobOracle): Observable[(LJob, RunData)] = {

    val drmJobOracle = DrmJobOracle.from(jobOracle, jobsById.mapValues(_.commandLineJob: LJob))
    
    import jobMonitor.monitor

    val jobsAndDrmStatuses: Observable[(DrmTaskId, DrmJobWrapper, DrmStatus)] = {
      val taskIds = jobsById.keys
      
      for {
        (tid, status) <- monitor(drmJobOracle)(taskIds)
        wrapper <- jobsById.get(tid).map(Observable(_)).getOrElse(Observable.empty)
      } yield {
        (tid, wrapper, status)
      }
    }
    
    toRunDatas(accountingClient, jobsAndDrmStatuses)
  }
  
  /**
   * Takes a bunch of CommandLineJobs, and groups them by their execution environments.  This has the effect
   * of groupign together jobs with the same Uger settings.  This is necessary so that we can run each group
   * of jobs as one Uger task array, and settings are per-task-array.
   */
  private[drm] def subChunksBySettings(jobs: Seq[CommandLineJob]): Map[DrmSettings, Seq[CommandLineJob]] = {
    import loamstream.util.Maps.Implicits._
    
    jobs.groupBy(_.initialSettings).collectKeys { 
      case drmSettings: DrmSettings => drmSettings
    }
  }
}

object DrmChunkRunner extends Loggable {

  type JobAndStatuses = (DrmJobWrapper, Observable[DrmStatus])

  private[drm] def notSuccess(status: DrmStatus): Boolean = status match {
    case DrmStatus.Done => false
    case DrmStatus.CommandResult(ec) => ExitCodes.isFailure(ec)
    case _ => true
  }
  
  private[drm] def getAccountingInfoFor(
      accountingClient: AccountingClient)(taskId: DrmTaskId): Task[Option[AccountingInfo]] = {
    
    val infoAttempt = accountingClient.getAccountingInfo(taskId).map(Option(_))
        
    infoAttempt.onErrorRecover[Option[AccountingInfo]] { case e => 
      warn(s"Error invoking accounting client for job with DRM id '$taskId': ${e.getMessage}", e)
    
      None
    }
  }
  
  private[drm] def toRunData(
      accountingClient: AccountingClient, 
      wrapper: DrmJobWrapper, 
      taskId: DrmTaskId)(s: DrmStatus): Observable[RunData] = {
    
    val infoOptT: Task[Option[AccountingInfo]] = {
      val statusDescription = s"${simpleNameOf[DrmStatus]} ${s}"
      
      if(s.isFinished && notSuccess(s)) {
        debug(s"${statusDescription} is finished and NOT a success, determining execution node and queue: $s")
        
        getAccountingInfoFor(accountingClient)(taskId)
      } else {
        debug(s"${statusDescription} is NOT finished or is a success, NOT determining execution node and queue: $s")
        
        Task.now(None)
      }
    }
    
    val resultT = for {
      infoOpt <- infoOptT
    } yield {
      RunData(
        job = wrapper.commandLineJob,
        settings = wrapper.drmSettings,
        jobStatus = toJobStatus(s),
        jobResult = toJobResult(s),
        resourcesOpt = infoOpt.map(_.resources),
        jobDirOpt = Option(wrapper.jobDir),
        terminationReasonOpt = infoOpt.flatMap(_.terminationReasonOpt))
    }
    
    Observable.from(resultT)
  }
  
  private[drm] def toRunDatas(
    accountingClient: AccountingClient, 
    //jobsAndDrmStatusesById: Map[DrmTaskId, JobAndStatuses])
    jobsAndDrmStatusesById: Observable[(DrmTaskId, DrmJobWrapper, DrmStatus)]): Observable[(LJob, RunData)] = {

    val drmJobsToExecutionObservables: Observable[(DrmJobWrapper, RunData)] = for {
      (taskId, wrapper, status) <- jobsAndDrmStatusesById
      if status.isFinished
      runData <- toRunData(accountingClient, wrapper, taskId)(status)
    } yield {
      wrapper -> runData
    }

    val jobsToExecutionObservables = drmJobsToExecutionObservables.map { case (jobWrapper, runData) => 
      (jobWrapper.commandLineJob, runData) 
    }
    
    implicit val eqTuple: Eq[(HasCommandLine, RunData)] = Eq.fromUniversalEquals

    jobsToExecutionObservables.distinctUntilChanged
  }

  private[drm] def makeAllFailureMap(
      jobs: Seq[DrmJobWrapper], 
      cause: Option[Throwable]): Observable[(LJob, RunData)] = {
    
    cause.foreach(e => error(s"Couldn't submit jobs to DRM system: ${e.getMessage}", e))

    val (result: JobResult, status: JobStatus) = cause match {
      case Some(e) => (JobResult.FailureWithException(e), JobStatus.FailedWithException)
      case None    => (JobResult.Failure, JobStatus.Failed)
    }

    def execution(jobWrapper: DrmJobWrapper): RunData = {
      RunData(
          job = jobWrapper.commandLineJob, 
          settings = jobWrapper.drmSettings,
          jobStatus = status, 
          jobResult = Option(result), 
          resourcesOpt = None, 
          jobDirOpt = Option(jobWrapper.jobDir),
          terminationReasonOpt = None)
    }

    Observable.from(jobs.map(wrapper => wrapper.commandLineJob -> execution(wrapper)))
  }
}
