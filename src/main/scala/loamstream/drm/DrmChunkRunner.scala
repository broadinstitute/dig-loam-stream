package loamstream.drm

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import DrmStatus.toJobResult
import DrmStatus.toJobStatus
import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.ExecuterHelpers
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
import rx.lang.scala.Observable
import loamstream.util.TimeUtils
import loamstream.util.ExitCodes


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
    sessionSource: SessionSource = SessionSource.Noop
  )(implicit ec: ExecutionContext) extends ChunkRunnerFor(environmentType) with 
        Terminable.StopsComponents with Loggable {

  require(environmentType.isUger || environmentType.isLsf, "Only UGER and LSF environments are supported")
  
  //TODO: FIXME
  loamstream.drm.uger.Sessions.init(sessionSource.getSession)
  
  import DrmChunkRunner._

  override protected val terminableComponents: Iterable[Terminable] = {
    val jobKillerTerminable: Terminable = Terminable(jobKiller.killAllJobs())
    
    Seq(jobMonitor, jobSubmitter, jobKillerTerminable)
  }

  /**
   * Run the provided jobs, using the provided predicate (`shouldRestart`) to decide whether to re-run them if they
   * fail.  Returns an Observable producing a map of jobs to Executions.
   *
   * NB: NoOpJobs are ignored.  Otherwise, this method expects that all the other jobs are CommandLineJobs, and
   * will throw otherwise.
   */
  override def run(
      jobs: Set[LJob], 
      jobOracle: JobOracle): Observable[Map[LJob, RunData]] = {

    debug(s"${getClass.getSimpleName}: Running ${jobs.size} jobs: ")
    jobs.foreach(job => debug(s"  $job"))

    require(
      jobs.forall(_.isInstanceOf[CommandLineJob]),
      s"For now, we only know how to run ${simpleNameOf[CommandLineJob]}s on a DRM system")

    // Filter out NoOpJob's
    val commandLineJobs = jobs.toSeq.collect { case clj: CommandLineJob => clj }

    //Group Jobs by their uger settings, and run each group.  This is necessary because the jobs in a group will
    //be run as 1 Uger task array, and Uger params are per-task-array.
    val resultsForSubChunks: Iterable[Observable[Map[LJob, RunData]]] = {
      import DrmTaskArray.fromCommandLineJobs
      
      val maxJobsPerTaskArray = drmConfig.maxNumJobsPerTaskArray
      
      for {
        (settings, rawJobs) <- subChunksBySettings(commandLineJobs)
        rawJobChunk <- rawJobs.sliding(maxJobsPerTaskArray, maxJobsPerTaskArray)
      } yield {
        val drmTaskArray = TimeUtils.time(s"Making DrmTaskArray with ${rawJobChunk.size} jobs", debug(_)) {
          fromCommandLineJobs(executionConfig, jobOracle, settings, drmConfig, pathBuilder, rawJobChunk)
        }
        
        runJobs(settings, drmTaskArray)
      }
    }

    if (resultsForSubChunks.isEmpty) { Observable.just(Map.empty) }
    else { Observables.merge(resultsForSubChunks) }
  }

  /**
   * Submits the provided CommandLineJobs and monitors them, resulting in an Observable producing a map of jobs to
   * Executions
   */
  private def runJobs(
    drmSettings: DrmSettings,
    drmTaskArray: DrmTaskArray): Observable[Map[LJob, RunData]] = {

    drmTaskArray.drmJobs match {
      case Nil => Observable.just(Map.empty)
      case drmJobs => jobSubmitter.submitJobs(drmSettings, drmTaskArray).flatMap(toRunDataStream(drmJobs, _))
    }
  }

  private def toRunDataStream(
    drmJobs: Seq[DrmJobWrapper],
    submissionResult: DrmSubmissionResult)(implicit ec: ExecutionContext): Observable[Map[LJob, RunData]] = {
    
    val commandLineJobs = drmJobs.map(_.commandLineJob)
    
    import loamstream.drm.DrmSubmissionResult._
    
    submissionResult match {
      case SubmissionSuccess(drmJobsByDrmId) => jobsToRunDatas(drmJobsByDrmId)
      case SubmissionFailure(e) => makeAllFailureMap(drmJobs, Some(e))
    }
  }
  
  private[drm] def jobsToRunDatas(
    jobsById: Map[DrmTaskId, DrmJobWrapper])(implicit ec: ExecutionContext): Observable[Map[LJob, RunData]] = {

    def statuses(taskIds: Iterable[DrmTaskId]): Map[DrmTaskId, Observable[DrmStatus]] = jobMonitor.monitor(taskIds)

    val jobsAndDrmStatusesById = combine(jobsById, statuses(jobsById.keys))

    toRunDatas(accountingClient, jobsAndDrmStatusesById)
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
  
  private[drm] def getAccountingInfoFor(accountingClient: AccountingClient)(taskId: DrmTaskId)
                                       (implicit ec: ExecutionContext): Future[Option[AccountingInfo]] = {
    
    val infoAttempt = accountingClient.getAccountingInfo(taskId)
        
    //For side effect only
    infoAttempt.recover {
      case e => warn(s"Error invoking accounting client for job with DRM id '$taskId': ${e.getMessage}", e)
    }
    
    infoAttempt.transformWith { 
      case attempt => Future.successful(attempt.toOption)
    }
  }
  
  private[drm] def toRunData(
      accountingClient: AccountingClient, 
      wrapper: DrmJobWrapper, 
      taskId: DrmTaskId)(s: DrmStatus)(implicit ec: ExecutionContext): Observable[RunData] = {
    
    val infoOptFuture: Future[Option[AccountingInfo]] = {
      if(s.isFinished && notSuccess(s)) {
        debug(s"${simpleNameOf[DrmStatus]} is finished, determining execution node and queue: $s")
        
        getAccountingInfoFor(accountingClient)(taskId)
      } else {
        debug(s"${simpleNameOf[DrmStatus]} is NOT finished, NOT determining execution node and queue: $s")
        
        Future.successful(None)
      }
    }
    
    val resultFuture = for {
      infoOpt <- infoOptFuture
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
    
    Observable.from(resultFuture)
  }
  
  private[drm] def toRunDatas(
    accountingClient: AccountingClient, 
    jobsAndDrmStatusesById: Map[DrmTaskId, JobAndStatuses])
    (implicit ec: ExecutionContext): Observable[Map[LJob, RunData]] = {

    val drmJobsToExecutionObservables: Iterable[(DrmJobWrapper, Observable[RunData])] = for {
      (taskId, (wrapper, drmJobStatuses)) <- jobsAndDrmStatusesById
    } yield {
      val runDataObs = drmJobStatuses.last.flatMap(toRunData(accountingClient, wrapper, taskId))

      wrapper -> runDataObs
    }

    val jobsToExecutionObservables = drmJobsToExecutionObservables.map { case (jobWrapper, obs) => 
      (jobWrapper.commandLineJob, obs) 
    }
    
    Observables.toMap(jobsToExecutionObservables)
  }

  private[drm] def makeAllFailureMap(
      jobs: Seq[DrmJobWrapper], 
      cause: Option[Throwable]): Observable[Map[LJob, RunData]] = {
    
    cause.foreach(e => error(s"Couldn't submit jobs to DRM system: ${e.getMessage}", e))

    val (result: JobResult, status: JobStatus) = cause match {
      case Some(e) => (JobResult.FailureWithException(e), JobStatus.FailedWithException)
      case None    => (JobResult.Failure, JobStatus.Failed)
    }

    val execution: DrmJobWrapper => RunData = { jobWrapper =>
      RunData(
          job = jobWrapper.commandLineJob, 
          settings = jobWrapper.drmSettings,
          jobStatus = status, 
          jobResult = Option(result), 
          resourcesOpt = None, 
          jobDirOpt = Option(jobWrapper.jobDir),
          terminationReasonOpt = None)
    }

    import loamstream.util.Maps.Implicits._
    import loamstream.util.Traversables.Implicits._
    
    Observable.just(jobs.mapTo(execution).mapKeys(_.commandLineJob))
  }

  /**
   * Takes two maps, and produces a new map containing keys that exist in both input maps, each mapped to
   * a tuple of the keys mapped to by that key in the input maps.  For example:
   * 
   * given: 
   * Map(
   *   jobId0 -> job0,
   *   jobId1 -> job1)
   *   
   * and
   * 
   * Map(
   *   jobId0 -> streamOfStatuses0
   *   jobId1 -> streamOfStatuses1)
   *   
   * return 
   * 
   * Map(
   *   jobId0 -> (job0, streamOfStatuses0)
   *   jobId1 -> (job1, streamOfStatuses1))
   *   
   * This is used to combine a map of jobs keyed on job ids with a map of job status streams keyed by id,
   * but is generic since it makes this implementation shorter, easier to test, and (IMO) easier to read. (-Clint)
   */
  private[drm] def combine[A, U, V](m1: Map[A, U], m2: Map[A, V]): Map[A, (U, V)] = {
    for {
      (a, u) <- m1
      v <- m2.get(a)
    } yield {
      a -> (u -> v)
    }
  }
}
