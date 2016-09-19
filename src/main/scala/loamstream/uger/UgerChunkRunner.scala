package loamstream.uger

import java.io.File
import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.execute.ChunkRunner
import loamstream.model.jobs.{LJob, NoOpJob}
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.uger.JobStatus._
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.Files
import loamstream.util.TimeEnrichments.time
import loamstream.conf.UgerConfig
import java.util.UUID
import loamstream.util.ObservableEnrichments

import loamstream.model.jobs.JobState.{Failed, Running}
import rx.lang.scala.Observable
import loamstream.util.Observables
import loamstream.model.jobs.JobState
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * @author clint
 * date: Jul 1, 2016
 * 
 * A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided DrmaaClient.
 * 
 * TODO: Make logging more fine-grained; right now, too much is at info level.
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    drmaaClient: DrmaaClient,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunner with Loggable {

  import UgerChunkRunner._

  override def maxNumJobs = ugerConfig.ugerMaxNumJobs

  override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {

    require(
      jobs.forall(isAcceptableJob),
      s"For now, we only know how to run ${classOf[CommandLineJob].getSimpleName}s on UGER")

    // Filter out NoOpJob's
    val commandLineJobs = jobs.filterNot(isNoOpJob).toSeq.collect { case clj: CommandLineJob => clj }

    if (commandLineJobs.nonEmpty) {
      val (ugerWorkDir, ugerScript) = makeUgerScript(commandLineJobs)

      val ugerLogFile: Path = ugerConfig.ugerLogFile

      val submissionResult = drmaaClient.submitJob(ugerScript, ugerLogFile, jobName, commandLineJobs.size)

      submissionResult match {
        case DrmaaClient.SubmissionSuccess(rawJobIds) => {
          handleSuccessfulSubmission(drmaaClient, pollingFrequencyInHz, commandLineJobs, rawJobIds)
        }
        case DrmaaClient.SubmissionFailure(e) => handleFailedSubmission(commandLineJobs, e)
      }
    } else {
      // Handle NoOp case or a case when no jobs were presented for some reason
      Observable.just(Map.empty)
    }
  }
  
  private[uger] def makeUgerScript(commandLineJobs: Seq[CommandLineJob]): (File, Path) = {
    val ugerWorkDir = ugerConfig.ugerWorkDir.toFile
    val ugerScript = createScriptFile(ScriptBuilder.buildFrom(commandLineJobs), ugerWorkDir)

    info(s"Made script '$ugerScript' from $commandLineJobs")
    
    (ugerWorkDir, ugerScript)
  }
}

object UgerChunkRunner extends Loggable {
  private[uger] def handleFailedSubmission(jobs: Seq[LJob], e: Exception): Observable[Map[LJob, Result]] = {
    jobs.foreach(_.updateAndEmitJobState(Failed))
          
    makeAllFailureMap(jobs, Some(e))
  }
  
  private[uger] def handleSuccessfulSubmission(
      drmaaClient: DrmaaClient,
      pollingFrequencyInHz: Double,
      jobs: Seq[CommandLineJob],
      rawJobIds: Seq[String])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {

    val jobsById = rawJobIds.zip(jobs).toMap
    
    for {
      (jobId, job) <- jobsById
    } {
      val status = drmaaClient.statusOf(jobId).getOrElse(JobStatus.Undetermined)
      
      job.updateAndEmitJobState(toJobState(status))
    }
          
    toResultMap(drmaaClient, pollingFrequencyInHz, jobsById)
  }

  private[uger] def toResultMap(
      drmaaClient: DrmaaClient,
      pollingFrequencyInHz: Double,
      jobsById: Map[String, LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {
    
    val poller = Poller.drmaa(drmaaClient)

    def statuses(jobId: String) = Jobs.monitor(poller, pollingFrequencyInHz)(jobId)

    val jobsToStates: Iterable[(LJob, Observable[JobState])] = for {
      (jobId, job) <- jobsById
    } yield {
      val states = statuses(jobId).map(toJobState)
      
      job -> states
    }

    for {
      (job, states) <- jobsToStates
    } {
      states.distinctUntilChanged.foreach(job.updateAndEmitJobState)
    }
    
    val jobsToFutureResults: Iterable[(LJob, Observable[Result])] = for {
      (job, jobStates) <- jobsToStates
    } yield {
      val resultObservable = jobStates.last.map(resultFrom(job))

      job -> resultObservable
    }

    Observables.toMap(jobsToFutureResults)
  }
  
  //TODO: do we need this?  Should it be something better?
  private[uger] def jobName: String = s"LoamStream-${UUID.randomUUID}"
  
  private[uger] def isCommandLineJob(job: LJob): Boolean = job match {
    case clj: CommandLineJob => true
    case _                   => false
  }

  private[uger] def isNoOpJob(job: LJob): Boolean = job match {
    case noj: NoOpJob => true
    case _            => false
  }

  private[uger] def isAcceptableJob(job: LJob): Boolean = isNoOpJob(job) || isCommandLineJob(job)

  private[uger] def resultFrom(job: LJob)(status: JobState): LJob.Result = {
    //TODO: Anything better; this was purely expedient
    if (status.isSuccess) {
      LJob.SimpleSuccess(s"$job")
    } else {
      LJob.SimpleFailure(s"$job")
    }
  }

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Observable[Map[LJob, Result]] = {
    val msg = cause match {
      case Some(e) => s"Couldn't submit jobs to UGER: ${e.getMessage}"
      case None    => "Couldn't submit jobs to UGER"
    }

    cause.foreach(e => error(msg, e))

    Observable.just(jobs.map(j => j -> SimpleFailure(msg)).toMap)
  }
  
  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)
    
    file
  }

  /**
   * Creates a script file in the *default temporary-file directory*, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String): Path = createScriptFile(contents, Files.tempFile(".sh"))

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String, directory: File): Path = {
    createScriptFile(contents, Files.tempFile(".sh", directory))
  }
}
