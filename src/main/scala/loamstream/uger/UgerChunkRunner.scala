package loamstream.uger

import java.io.File
import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.execute.ChunkRunner
import loamstream.model.jobs.{ LJob, NoOpJob }
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.uger.JobStatus._
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.Files
import loamstream.util.TimeEnrichments.time
import loamstream.conf.UgerConfig
import java.util.UUID

import loamstream.util.ObservableEnrichments
import loamstream.model.jobs.JobState.{ Failed, Running }
import loamstream.model.jobs.JobState
import loamstream.util.Traversables
import rx.lang.scala.Scheduler
import rx.schedulers.Schedulers
import loamstream.util.RxSchedulers

/**
 * @author clint
 *         date: Jul 1, 2016
 *
 *         A ChunkRunner that runs groups of command line jobs as UGER task arrays, via the provided DrmaaClient.
 *
 *         TODO: Make logging more fine-grained; right now, too much is at info level.
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    drmaaClient: DrmaaClient,
    jobOps: JobMonitor,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunner with Loggable {

  import UgerChunkRunner._

  override def maxNumJobs = ugerConfig.ugerMaxNumJobs

  override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {

    debug(s"Running: ")
    leaves.foreach(job => debug(s"  $job"))

    require(
      leaves.forall(isAcceptableJob),
      s"For now, we only know how to run ${classOf[CommandLineJob].getSimpleName}s on UGER")

    // Filter out NoOpJob's
    val leafCommandLineJobs = leaves.toSeq.filterNot(isNoOpJob).collect { case clj: CommandLineJob => clj }

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

          toResultMap(drmaaClient, jobsById)(context)
        }
        case DrmaaClient.SubmissionFailure(e) => {
          leafCommandLineJobs.foreach(_.updateAndEmitJobState(Failed))
          makeAllFailureMap(leafCommandLineJobs, Some(e))
        }
      }
    } else {
      // Handle NoOp case or a case when no jobs were presented for some reason
      Future.successful(Map.empty)
    }
  }

  private[uger] def toResultMap(
      drmaaClient: DrmaaClient, 
      jobsById: Map[String, CommandLineJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {

    def statuses(jobIds: Iterable[String]) = time(s"Calling Jobs.monitor(${jobIds.mkString(",")})", trace(_)) {
      jobOps.monitor(jobIds)
    }

    import ObservableEnrichments._

    val jobsAndStatusesById = combine(jobsById, statuses(jobsById.keys))

    val jobsToFutureResults: Iterable[(LJob, Future[JobState])] = for {
      (jobId, (job, jobStatuses)) <- jobsAndStatusesById
      _ = jobStatuses.foreach(status => job.updateAndEmitJobState(toJobState(status)))
      futureResult = jobStatuses.lastAsFuture.map(JobStatus.toJobState)
    } yield {
      job -> futureResult
    }

    Futures.toMap(jobsToFutureResults)
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

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Future[Map[LJob, JobState]] = {
    val failure = cause match {
      case Some(e) => JobState.FailedWithException(e)
      case None    => JobState.Failed
    }

    cause.foreach(e => error(s"Couldn't submit jobs to UGER: ${e.getMessage}", e))

    import Traversables.Implicits._

    Future.successful(jobs.mapTo(_ => failure))
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

  private[uger] def combine[A, U, V](m1: Map[A, U], m2: Map[A, V]): Map[A, (U, V)] = {
    Map.empty[A, (U, V)] ++ (for {
      (a, u) <- m1.toIterable
      v <- m2.get(a)
    } yield {
      a -> (u -> v)
    })
  }
}
