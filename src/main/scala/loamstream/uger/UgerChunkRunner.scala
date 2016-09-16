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
    val leafCommandLineJobs = jobs.filterNot(isNoOpJob).toSeq.collect { case clj: CommandLineJob => clj }

    if (leafCommandLineJobs.nonEmpty) {
      val ugerWorkDir = ugerConfig.ugerWorkDir.toFile
      val ugerScript = createScriptFile(ScriptBuilder.buildFrom(leafCommandLineJobs), ugerWorkDir)

      info(s"Made script '$ugerScript' from $leafCommandLineJobs")

      val ugerLogFile: Path = ugerConfig.ugerLogFile

      //TODO: do we need this?  Should it be something better?
      val jobName: String = s"LoamStream-${UUID.randomUUID}"

      val submissionResult = drmaaClient.submitJob(ugerScript, ugerLogFile, jobName, leafCommandLineJobs.size)

      submissionResult match {
        case DrmaaClient.SubmissionSuccess(rawJobIds) => {
          leafCommandLineJobs.foreach(_.updateAndEmitJobState(Running))

          val jobsById = rawJobIds.zip(leafCommandLineJobs).toMap
          
          toResultMap(drmaaClient, jobsById)
        }
        case DrmaaClient.SubmissionFailure(e) => {
          leafCommandLineJobs.foreach(_.updateAndEmitJobState(Failed))
          
          makeAllFailureMap(leafCommandLineJobs, Some(e))
        }
      }
    } else {
      // Handle NoOp case or a case when no jobs were presented for some reason
      Observable.just(Map.empty)
    }
  }

  private[uger] def toResultMap(
      drmaaClient: DrmaaClient, 
      jobsById: Map[String, LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]] = {
    
    val poller = Poller.drmaa(drmaaClient)

    def statuses(jobId: String) = time(s"Job '$jobId': Calling Jobs.monitor()", debug(_)) {
      Jobs.monitor(poller, pollingFrequencyInHz)(jobId)
    }

    import ObservableEnrichments._
    
    val jobsToFutureResults: Iterable[(LJob, Observable[Result])] = for {
      (jobId, job) <- jobsById
    } yield {
      //TODO: TRY THIS OUT
      //_ = jobStatuses.foreach(status => job.updateAndEmitJobState(toJobState(status)))
      
      val jobStatuses = statuses(jobId)
      
      jobStatuses.map(toJobState).foreach(job.updateAndEmitJobState)
      
      val resultObservable = jobStatuses.last.map(resultFrom(job))

      job -> resultObservable
    }

    Observables.toMap(jobsToFutureResults)
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

  private[uger] def isAcceptableJob(job: LJob): Boolean = isNoOpJob(job) || isCommandLineJob(job)

  private[uger] def resultFrom(job: LJob)(status: JobStatus): LJob.Result = {
    //TODO: Anything better; this was purely expedient
    if (status.isDone) {
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
