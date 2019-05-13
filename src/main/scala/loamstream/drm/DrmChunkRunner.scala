package loamstream.drm

import DrmStatus.toJobResult
import DrmStatus.toJobStatus
import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.Environment
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobStatus.Failed
import loamstream.model.jobs.JobStatus.Running
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.Classes.simpleNameOf
import loamstream.util.CompositeException
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.Throwables
import rx.lang.scala.Observable
import loamstream.model.jobs.TerminationReason


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
    accountingClient: AccountingClient) extends ChunkRunnerFor(environmentType) with Terminable with Loggable {

  require(environmentType.isUger || environmentType.isLsf, "Only UGER and LSF environments are supported")
  
  import DrmChunkRunner._

  override def stop(): Unit = {
    val failures = Throwables.collectFailures(
        jobMonitor.stop _, 
        jobSubmitter.stop _)
    
    if(failures.nonEmpty) {
      throw new CompositeException(failures)
    }
  }

  override def maxNumJobs: Int = drmConfig.maxNumJobs

  /**
   * Run the provided jobs, using the provided predicate (`shouldRestart`) to decide whether to re-run them if they
   * fail.  Returns an Observable producing a map of jobs to Executions.
   *
   * NB: NoOpJobs are ignored.  Otherwise, this method expects that all the other jobs are CommandLineJobs, and
   * will throw otherwise.
   */
  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {

    debug(s"Running: ")
    jobs.foreach(job => debug(s"  $job"))

    require(
      jobs.forall(_.isInstanceOf[CommandLineJob]),
      s"For now, we only know how to run ${simpleNameOf[CommandLineJob]}s on a DRM system")

    // Filter out NoOpJob's
    val commandLineJobs = jobs.toSeq.collect { case clj: CommandLineJob => clj }

    //Group Jobs by their uger settings, and run each group.  This is necessary because the jobs in a group will
    //be run as 1 Uger task array, and Uger params are per-task-array.
    val resultsForSubChunks: Iterable[Observable[Map[LJob, RunData]]] = {
      for {
        (settings, rawJobs) <- subChunksBySettings(commandLineJobs)
        drmTaskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, settings, drmConfig, pathBuilder, rawJobs)
      } yield {
        runJobs(settings, drmTaskArray, shouldRestart)
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
    drmTaskArray: DrmTaskArray,
    shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {

    drmTaskArray.drmJobs match {
      case Nil => Observable.just(Map.empty)
      case drmJobs => {
        val submissionResult = jobSubmitter.submitJobs(drmSettings, drmTaskArray)

        toRunDataStream(drmJobs, submissionResult, shouldRestart)
      }
    }
  }

  private def toRunDataStream(
    drmJobs: Seq[DrmJobWrapper],
    submissionResult: DrmSubmissionResult,
    shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    val commandLineJobs = drmJobs.map(_.commandLineJob)

    updateJobStatusesOnSubmission(commandLineJobs, submissionResult, shouldRestart)
    
    import loamstream.drm.DrmSubmissionResult._
    
    submissionResult match {
      case SubmissionSuccess(drmJobsByDrmId) => jobsToRunDatas(shouldRestart, drmJobsByDrmId)
      case SubmissionFailure(e) => makeAllFailureMap(drmJobs, Some(e))
    }
  }
  
  private[drm] def jobsToRunDatas(
    shouldRestart: LJob => Boolean,
    jobsById: Map[String, DrmJobWrapper]): Observable[Map[LJob, RunData]] = {

    def statuses(jobIds: Iterable[String]): Map[String, Observable[DrmStatus]] = jobMonitor.monitor(jobIds)

    val jobsAndDrmStatusesById = combine(jobsById, statuses(jobsById.keys))

    toRunDatas(accountingClient, shouldRestart, jobsAndDrmStatusesById)
  }
  
  /**
   * Takes a bunch of CommandLineJobs, and groups them by their execution environments.  This has the effect
   * of groupign together jobs with the same Uger settings.  This is necessary so that we can run each group
   * of jobs as one Uger task array, and settings are per-task-array.
   */
  private[drm] def subChunksBySettings(jobs: Seq[CommandLineJob]): Map[DrmSettings, Seq[CommandLineJob]] = {
    import loamstream.util.Maps.Implicits._
    
    jobs.groupBy(_.executionEnvironment).collectKeys { 
      case Environment.Uger(ugerSettings) => ugerSettings
      case Environment.Lsf(lsfSettings) => lsfSettings
    }
  }
}

object DrmChunkRunner extends Loggable {

  type JobAndStatuses = (DrmJobWrapper, Observable[DrmStatus])

  private[drm] def updateJobStatusesOnSubmission(
      commandLineJobs: Seq[HasCommandLine],
      submissionResult: DrmSubmissionResult,
      shouldRestart: LJob => Boolean): Unit = {
    
    import loamstream.drm.DrmSubmissionResult._
    
    submissionResult match {
      case SubmissionSuccess(drmJobsByDrmId) => {
        val actuallySubmittedJobs = drmJobsByDrmId.values.map(_.commandLineJob).toSet.intersect(commandLineJobs.toSet)
        
        actuallySubmittedJobs.foreach(_.transitionTo(Running))
      }
      case SubmissionFailure(e) => commandLineJobs.foreach(handleFailureStatus(shouldRestart, Failed))
    }
  }
  
  private[drm] def getAccountingInfoFor(accountingClient: AccountingClient)(jobId: String): Option[AccountingInfo] = {
    val infoAttempt = accountingClient.getAccountingInfo(jobId)
        
    //For side effect only
    infoAttempt.recover {
      case e => warn(s"Error invoking accounting client for job with DRM id '$jobId': ${e.getMessage}", e)
    }
    
    infoAttempt.toOption
  }
  
  private[drm] def toRunData(
      accountingClient: AccountingClient, 
      wrapper: DrmJobWrapper, 
      jobId: String)(s: DrmStatus): RunData = {
    
    val infoOpt: Option[AccountingInfo] = {
      if(s.isFinished) {
        debug(s"${simpleNameOf[DrmStatus]} is finished, determining execution node and queue: $s")
        
        getAccountingInfoFor(accountingClient)(jobId)
      } else {
        debug(s"${simpleNameOf[DrmStatus]} is NOT finished, NOT determining execution node and queue: $s")
        
        None
      }
    }
    
    RunData(
        job = wrapper.commandLineJob,
        settings = wrapper.drmSettings,
        jobStatus = toJobStatus(s),
        jobResult = toJobResult(s),
        resourcesOpt = infoOpt.map(_.resources),
        outputStreamsOpt = Option(wrapper.outputStreams),
        terminationReasonOpt = infoOpt.flatMap(_.terminationReasonOpt))
  }
  
  private[drm] def toRunDatas(
    accountingClient: AccountingClient, 
    shouldRestart: LJob => Boolean,
    jobsAndDrmStatusesById: Map[String, JobAndStatuses]): Observable[Map[LJob, RunData]] = {

    val drmJobsToExecutionObservables: Iterable[(DrmJobWrapper, Observable[RunData])] = for {
      (jobId, (wrapper, drmJobStatuses)) <- jobsAndDrmStatusesById
    } yield {
      //NB: Important: Jobs must be transitioned to new states by ChunkRunners like us.
      drmJobStatuses.distinct.foreach(handleDrmStatus(shouldRestart, wrapper.commandLineJob))

      val runDataObs = drmJobStatuses.last.map(toRunData(accountingClient, wrapper, jobId))

      wrapper -> runDataObs
    }

    val jobsToExecutionObservables = drmJobsToExecutionObservables.map { case (jobWrapper, obs) => 
      (jobWrapper.commandLineJob, obs) 
    }
    
    Observables.toMap(jobsToExecutionObservables)
  }

  private[drm] def handleDrmStatus(shouldRestart: LJob => Boolean, job: LJob)(ds: DrmStatus): Unit = {
    val jobStatus = toJobStatus(ds)

    if (jobStatus.isFailure) {
      debug(s"Handling failure status $jobStatus (was DRM status $ds) for job $job")

      handleFailureStatus(shouldRestart, jobStatus)(job)
    } else {
      job.transitionTo(jobStatus)
    }
  }

  private[drm] def handleFailureStatus(shouldRestart: LJob => Boolean, failureStatus: JobStatus)(job: LJob): Unit = {

    val status = ExecuterHelpers.determineFailureStatus(shouldRestart, failureStatus, job)

    trace(s"$job transitioning to: $status (Non-terminal failure status: $failureStatus)")

    job.transitionTo(status)
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
          outputStreamsOpt = Option(jobWrapper.outputStreams),
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
