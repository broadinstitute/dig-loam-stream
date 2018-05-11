package loamstream.drm.uger

import loamstream.conf.ExecutionConfig
import loamstream.conf.UgerConfig
import loamstream.drm.DrmJobWrapper
import loamstream.drm.DrmStatus
import loamstream.drm.DrmStatus.toJobResult
import loamstream.drm.DrmStatus.toJobStatus
import loamstream.drm.DrmSubmissionResult
import loamstream.drm.DrmTaskArray
import loamstream.drm.JobMonitor
import loamstream.drm.JobSubmitter
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobStatus.Failed
import loamstream.model.jobs.JobStatus.Running
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Classes.simpleNameOf
import loamstream.util.CompositeException
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.Throwables
import loamstream.util.TimeUtils.time
import rx.lang.scala.Observable
import loamstream.conf.DrmConfig
import loamstream.drm.PathBuilder

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
    jobMonitor: JobMonitor) extends ChunkRunnerFor(environmentType) with Terminable with Loggable {

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
        (drmSettings, commandLineJobs) <- subChunksBySettings(commandLineJobs)
        drmTaskArray = DrmTaskArray.fromCommandLineJobs(executionConfig, drmConfig, pathBuilder, commandLineJobs)
      } yield {
        runJobs(drmSettings, drmTaskArray, shouldRestart)
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

        toExecutionStream(drmJobs, submissionResult, shouldRestart)
      }
    }
  }

  private def toExecutionStream(
    drmJobs: Seq[DrmJobWrapper],
    submissionResult: DrmSubmissionResult,
    shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    
    val commandLineJobs = drmJobs.map(_.commandLineJob)
    
    import loamstream.drm.DrmSubmissionResult._
    
    submissionResult match {

      case SubmissionSuccess(drmJobsByDrmId) => {
        commandLineJobs.foreach(_.transitionTo(Running))

        jobsToRunDatas(shouldRestart, drmJobsByDrmId)
      }
      case SubmissionFailure(e) => {
        commandLineJobs.foreach(handleFailureStatus(shouldRestart, Failed))

        makeAllFailureMap(drmJobs, Some(e))
      }
    }
  }

  private[uger] def jobsToRunDatas(
    shouldRestart: LJob => Boolean,
    jobsById: Map[String, DrmJobWrapper]): Observable[Map[LJob, RunData]] = {

    def statuses(jobIds: Iterable[String]): Map[String, Observable[DrmStatus]] = {
      time(s"Calling JobMonitor.monitor(${jobIds.mkString(",")})", trace(_)) {
        jobMonitor.monitor(jobIds)
      }
    }

    val jobsAndDrmStatusesById = combine(jobsById, statuses(jobsById.keys))

    toRunDatas(shouldRestart, jobsAndDrmStatusesById)
  }
  
  /**
   * Takes a bunch of CommandLineJobs, and groups them by their execution environments.  This has the effect
   * of groupign together jobs with the same Uger settings.  This is necessary so that we can run each group
   * of jobs as one Uger task array, and settings are per-task-array.
   */
  private[uger] def subChunksBySettings(jobs: Seq[CommandLineJob]): Map[DrmSettings, Seq[CommandLineJob]] = {
    import loamstream.model.execute.Environment.Uger
    import loamstream.model.execute.Environment.Lsf
    import loamstream.util.Maps.Implicits._

    jobs.groupBy(_.executionEnvironment).collectKeys { 
      case Uger(ugerSettings) => ugerSettings
      case Lsf(lsfSettings) => lsfSettings
    }
  }
}

object DrmChunkRunner extends Loggable {

  type JobAndStatuses = (DrmJobWrapper, Observable[DrmStatus])

  private[uger] def toRunDatas(
    shouldRestart: LJob => Boolean,
    jobsAndDrmStatusesById: Map[String, JobAndStatuses]): Observable[Map[LJob, RunData]] = {

    val drmJobsToExecutionObservables: Iterable[(DrmJobWrapper, Observable[RunData])] = for {
      (jobId, (wrapper, drmJobStatuses)) <- jobsAndDrmStatusesById
    } yield {
      //NB: Important: Jobs must be transitioned to new states by ChunkRunners like us.
      drmJobStatuses.distinct.foreach(handleDrmStatus(shouldRestart, wrapper.commandLineJob))

      def toRunData(s: DrmStatus): RunData = {
        RunData(
            job = wrapper.commandLineJob,
            jobStatus = toJobStatus(s),
            jobResult = toJobResult(s),
            resourcesOpt = None, //NB: This will be filled in later, if possible
            outputStreamsOpt = Option(wrapper.outputStreams))
      }
      
      val runDataObs = drmJobStatuses.last.map(toRunData)

      wrapper -> runDataObs
    }

    val jobsToExecutionObservables = drmJobsToExecutionObservables.map { case (jobWrapper, obs) => 
      (jobWrapper.commandLineJob, obs) 
    }
    
    Observables.toMap(jobsToExecutionObservables)
  }

  private[uger] def handleDrmStatus(shouldRestart: LJob => Boolean, job: LJob)(ds: DrmStatus): Unit = {
    val jobStatus = toJobStatus(ds)

    if (jobStatus.isFailure) {
      debug(s"Handling failure status $jobStatus (was DRM status $ds) for job $job")

      handleFailureStatus(shouldRestart, jobStatus)(job)
    } else {
      job.transitionTo(jobStatus)
    }
  }

  private[uger] def handleFailureStatus(shouldRestart: LJob => Boolean, failureStatus: JobStatus)(job: LJob): Unit = {

    val status = ExecuterHelpers.determineFailureStatus(shouldRestart, failureStatus, job)

    trace(s"$job transitioning to: $status (Non-terminal failure status: $failureStatus)")

    job.transitionTo(status)
  }

  private[uger] def makeAllFailureMap(
      jobs: Seq[DrmJobWrapper], 
      cause: Option[Exception]): Observable[Map[LJob, RunData]] = {
    
    cause.foreach(e => error(s"Couldn't submit jobs to DRM system: ${e.getMessage}", e))

    val (result: JobResult, status: JobStatus) = cause match {
      case Some(e) => (JobResult.FailureWithException(e), JobStatus.FailedWithException)
      case None    => (JobResult.Failure, JobStatus.Failed)
    }

    val execution: DrmJobWrapper => RunData = { jobWrapper =>
      RunData(
          job = jobWrapper.commandLineJob, 
          jobStatus = status, 
          jobResult = Option(result), 
          resourcesOpt = None, 
          outputStreamsOpt = Option(jobWrapper.outputStreams))
    }

    import loamstream.util.Maps.Implicits._
    import loamstream.util.Traversables.Implicits._

    Observable.just(jobs.mapTo(execution).mapKeys(_.commandLineJob))
  }

  private[uger] def combine[A, U, V](m1: Map[A, U], m2: Map[A, V]): Map[A, (U, V)] = {
    for {
      (a, u) <- m1
      v <- m2.get(a)
    } yield {
      a -> (u -> v)
    }
  }
}
