package loamstream.uger

import java.io.File
import java.nio.file.Path
import java.util.UUID

import loamstream.conf.UgerConfig
import loamstream.model.execute.ChunkRunnerFor
import loamstream.model.jobs.JobStatus.{Failed, Running}
import loamstream.model.jobs._
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.uger.UgerStatus.{toJobResult, toJobStatus}
import loamstream.util.Classes.simpleNameOf
import loamstream.util.Files
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.Terminable
import loamstream.util.TimeUtils.time
import rx.lang.scala.Observable
import loamstream.model.execute.ExecuterHelpers
import loamstream.model.execute.EnvironmentType
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.time.ZoneId
import loamstream.model.execute.Environment
import loamstream.util.Maps
import loamstream.model.execute.UgerSettings

/**
 * @author clint
 *         date: Jul 1, 2016
 *
 *         A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided JobSubmitter.
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    jobSubmitter: JobSubmitter,
    jobMonitor: JobMonitor,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunnerFor(EnvironmentType.Uger) with Terminable with Loggable {

  import UgerChunkRunner._

  override def stop(): Unit = jobMonitor.stop()
  
  override def maxNumJobs = ugerConfig.maxNumJobs

  /**
   * Run the provided jobs, using the provided predicate (`shouldRestart`) to decide whether to re-run them if they 
   * fail.  Returns an Observable producing a map of jobs to Executions.
   * 
   * NB: NoOpJobs are ignored.  Otherwise, this method expects that all the other jobs are CommandLineJobs, and
   * will throw otherwise.
   */
  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {

    debug(s"Running: ")
    jobs.foreach(job => debug(s"  $job"))

    require(
      jobs.forall(isAcceptableJob),
      s"For now, we only know how to run ${simpleNameOf[CommandLineJob]}s on UGER")

    // Filter out NoOpJob's
    val commandLineJobs = jobs.toSeq.filterNot(isNoOpJob).collect { case clj: CommandLineJob => clj }

    //Group Jobs by their uger settings, and run each group.  This is necessary because the jobs in a group will
    //be run as 1 Uger task array, and Uger params are per-task-array.
    val resultsForSubChunks: Iterable[Observable[Map[LJob, Execution]]] = {
      for {
        (ugerSettings, ugerJobs) <- subChunksBySettings(commandLineJobs) 
      } yield {
        runJobs(ugerSettings, ugerJobs, shouldRestart) 
      }
    }
    
    if(resultsForSubChunks.isEmpty) { Observable.just(Map.empty) }
    else { Observables.merge(resultsForSubChunks) }
  }
  
  /**
   * Submits the provided CommandLineJobs and monitors them, resulting in an Observable producing a map of jobs to
   * Executions
   */
  private def runJobs(
      ugerSettings: UgerSettings, 
      ugerJobs: Seq[CommandLineJob],
      shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = {

    ugerJobs match {
      case Nil => Observable.just(Map.empty)
      case _ => {
        val submissionResult = jobSubmitter.submitJobs(ugerSettings, ugerJobs)

        toExecutionStream(ugerJobs, submissionResult, shouldRestart)
      }
    }
  }
  
  private def toExecutionStream(
      commandLineJobs: Seq[CommandLineJob], 
      submissionResult: DrmaaClient.SubmissionResult,
      shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]] = submissionResult match {

    case DrmaaClient.SubmissionSuccess(rawJobIds) => {
      commandLineJobs.foreach(_.transitionTo(Running))

      val jobsById = rawJobIds.zip(commandLineJobs).toMap

      jobsToExecutions(shouldRestart, jobsById)
    }
    case DrmaaClient.SubmissionFailure(e) => {
      commandLineJobs.foreach(handleFailureStatus(shouldRestart, Failed))

      makeAllFailureMap(commandLineJobs, Some(e))
    }
  }
  
  private[uger] def jobsToExecutions(
      shouldRestart: LJob => Boolean,
      jobsById: Map[String, CommandLineJob]): Observable[Map[LJob, Execution]] = {

    def statuses(jobIds: Iterable[String]): Map[String, Observable[UgerStatus]] = {
      time(s"Calling Jobs.monitor(${jobIds.mkString(",")})", trace(_)) {
        jobMonitor.monitor(jobIds)
      }
    }

    val jobsAndUgerStatusesById = combine(jobsById, statuses(jobsById.keys))
    
    toExecutions(shouldRestart, jobsAndUgerStatusesById)
  }
}

object UgerChunkRunner extends Loggable {
  private[uger] def isCommandLineJob(job: LJob): Boolean = job match {
    case clj: CommandLineJob => true
    case _                   => false
  }

  private[uger] def isNoOpJob(job: LJob): Boolean = job match {
    case noj: NoOpJob => true
    case _            => false
  }
  
  type JobAndStatuses = (LJob, Observable[UgerStatus])
  
  private[uger] def toExecutions(
      shouldRestart: LJob => Boolean, 
      jobsAndUgerStatusesById: Map[String, JobAndStatuses]): Observable[Map[LJob, Execution]] = {
    
    val ugerJobsToExecutionObservables: Iterable[(LJob, Observable[Execution])] = for {
      (jobId, (job, ugerJobStatuses)) <- jobsAndUgerStatusesById
    } yield {
      //NB: Important: Jobs must be transitioned to new states by ChunkRunners like us.
      ugerJobStatuses.distinct.foreach(handleUgerStatus(shouldRestart, job))
      
      val executionObs = ugerJobStatuses.last.map(s => Execution.from(job, toJobStatus(s), toJobResult(s)))
      
      job -> executionObs
    }

    Observables.toMap(ugerJobsToExecutionObservables)
  }
  
  private[uger] def handleUgerStatus(shouldRestart: LJob => Boolean, job: LJob)(us: UgerStatus): Unit = {
    val jobStatus = toJobStatus(us)
    
    if(jobStatus.isFailure) { handleFailureStatus(shouldRestart, jobStatus)(job) }
    else { job.transitionTo(jobStatus) }
  }
  
  private[uger] def handleFailureStatus(shouldRestart: LJob => Boolean, failureStatus: JobStatus)(job: LJob): Unit = {
    
    val status = ExecuterHelpers.determineFailureStatus(shouldRestart, failureStatus, job)
    
    trace(s"$job transitioning to: $status (Non-terminal failure status: $failureStatus)")
    
    job.transitionTo(status)
  }

  private[uger] def isAcceptableJob(job: LJob): Boolean = isNoOpJob(job) || isCommandLineJob(job)

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Observable[Map[LJob, Execution]] = {
    cause.foreach(e => error(s"Couldn't submit jobs to UGER: ${e.getMessage}", e))

    val (result, status) = cause match {
      case Some(e) => (JobResult.FailureWithException(e), JobStatus.FailedWithException)
      case None    => (JobResult.Failure, JobStatus.Failed)
    }

    val execution: LJob => Execution = job => Execution.from(job, status, Some(result))

    import loamstream.util.Traversables.Implicits._

    Observable.just(jobs.mapTo(execution))
  }

  private[uger] def combine[A, U, V](m1: Map[A, U], m2: Map[A, V]): Map[A, (U, V)] = {
    Map.empty[A, (U, V)] ++ (for {
      (a, u) <- m1.toIterable
      v <- m2.get(a)
    } yield {
      a -> (u -> v)
    })
  }
  
  /**
   * Takes a bunch of CommandLineJobs, and groups them by their execution environments.  This has the effect
   * of groupign together jobs with the same Uger settings.  This is necessary so that we can run each group
   * of jobs as one Uger task array, and settings are per-task-array.
   */
  private[uger] def subChunksBySettings(jobs: Seq[CommandLineJob]): Map[UgerSettings, Seq[CommandLineJob]] = {
    import Environment.Uger
    import Maps.Implicits._
      
    jobs.groupBy(_.executionEnvironment).collectKeys { case Uger(ugerSettings) => ugerSettings }
  }
}
