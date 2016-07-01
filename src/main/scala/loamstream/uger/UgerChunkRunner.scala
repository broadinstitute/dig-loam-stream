package loamstream.uger

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ChunkRunner
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.LJob.SimpleFailure
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Futures
import loamstream.util.Loggable
import monix.execution.Scheduler
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.util.Files
import loamstream.conf.ImputationConfig
import loamstream.conf.UgerConfig

/**
 * @author clint
 * date: Jul 1, 2016
 */
final case class UgerChunkRunner(
    ugerConfig: UgerConfig,
    drmaaClient: DrmaaClient,
    pollingFrequencyInHz: Double = 1.0) extends ChunkRunner with Loggable {

  import UgerChunkRunner._

  override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {

    require(
      leaves.forall(isCommandLineJob),
      s"For now, we only know how to run ${classOf[CommandLineJob].getSimpleName}s on UGER")

    //TODO: Work with any CommandLineJob
    val leafCommandLineJobs = leaves.toSeq.collect { case clj: CommandLineBuilderJob => clj }

    val ugerScript = createScriptFile(ScriptBuilder.buildFrom(leafCommandLineJobs))

    val ugerLogFile: Path = ugerConfig.ugerLogFile

    //TODO: 
    val jobName: String = s"LoamStream-${System.currentTimeMillis}"

    val submissionResult = drmaaClient.submitJob(ugerScript, ugerLogFile, jobName)

    submissionResult match {
      case DrmaaClient.BulkJobSubmissionResult(rawJobIds) => {
        import monix.execution.Scheduler.Implicits.global

        toResultMap(drmaaClient, leafCommandLineJobs, rawJobIds)
      }
      case DrmaaClient.Failure(e) => makeAllFailureMap(leafCommandLineJobs, Some(e))
      case _                      => makeAllFailureMap(leafCommandLineJobs, None)
    }
  }

  private[uger] def toResultMap(drmaaClient: DrmaaClient, jobs: Seq[LJob], rawJobIds: Seq[Any])(implicit scheduler: Scheduler): Future[Map[LJob, Result]] = {
    val jobIds = rawJobIds.map(_.toString)

    val jobsById = jobIds.zip(jobs).toMap

    val poller = Poller.drmaa(drmaaClient)

    def statuses(jobId: String) = Jobs.monitor(poller, pollingFrequencyInHz)(jobId)

    val jobsToFutureResults: Iterable[(LJob, Future[Result])] = for {
      jobId <- jobIds
      job = jobsById(jobId)
      futureResult = statuses(jobId).lastL.runAsync.collect { case Some(status) => resultFrom(status) }
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

  private[uger] def resultFrom(status: JobStatus): LJob.Result = {
    //TODO
    if (status.isDone) {
      LJob.SimpleSuccess("") //TODO
    } else {
      LJob.SimpleFailure("") //TODO
    }
  }

  private[uger] def makeAllFailureMap(jobs: Seq[LJob], cause: Option[Exception]): Future[Map[LJob, Result]] = {
    val msg = cause match {
      case Some(e) => s"Couldn't submit jobs to UGER: ${e.getMessage}"
      case None    => "Couldn't submit jobs to UGER"
    }

    cause.foreach(e => error(msg, e))

    Future.successful(jobs.map(j => j -> SimpleFailure(msg)).toMap)
  }
  
  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)
    
    file
  }
  
  //TODO:
  private[uger] def createScriptFile(contents: String): Path = createScriptFile(contents, Files.tempFile(".sh"))
}