package loamstream.drm.slurm

import scala.util.Try

import loamstream.drm.DrmStatus
import loamstream.drm.DrmTaskId
import loamstream.drm.Poller
import loamstream.model.jobs.DrmJobOracle
import monix.reactive.Observable
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import scala.collection.compat._
import loamstream.util.Observables
import loamstream.drm.RateLimitedPoller
import loamstream.drm.RateLimitedPoller.PollResultsForInvocation
import SqueuePoller.Params
import loamstream.conf.ExecutionConfig
import monix.execution.Scheduler
import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.util.Options
import scala.util.Success
import loamstream.drm.DrmSystem
import loamstream.util.FileMonitor

/**
 * @author clint
 * May 18, 2021
 *
 * Asynchronously inquire about the status of one or more jobs by invoking `squeue`/
 *
 * @param taskIds the ids of the tasks to inquire about
 * @return a map of task ids to attempts at that task's status
 */
final class SqueuePoller private[slurm] (
    pollingFn: CommandInvoker.Async[Params],
    commandName: String = "squeue",
    fileMonitor: FileMonitor) extends RateLimitedPoller[Params](commandName, pollingFn, fileMonitor) {

  protected override def toParams(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Params = drmTaskIds
  
  import SqueuePoller.PollResult
  
  override protected def getStatusesByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId])
     (runResults: RunResults.Successful): PollResultsForInvocation = {
    
    val outputLines = runResults.stdout
    
    val statusAttemptsById = outputLines.iterator.map(SqueuePoller.parseDataLine).map { //TODO 
      case Success((drmTaskId, drmStatus)) => (drmTaskId, Success(drmStatus))
    }.toMap
    
    PollResultsForInvocation(runResults, statusAttemptsById)
  }
}

object SqueuePoller extends RateLimitedPoller.Companion[Iterable[DrmTaskId], SqueuePoller] with Loggable {
  def fromExecutable(
      actualExecutable: String,
      pollingFrequencyInHz: Double,
      executionConfig: ExecutionConfig,
      scheduler: Scheduler): SqueuePoller = {
    
    val invoker = commandInvoker(
        pollingFrequencyInHz, 
        actualExecutable, 
        makeTokens(actualExecutable)(_))(scheduler, this) 
    
    import executionConfig.{ executionPollingFrequencyInHz, maxWaitTimeForOutputs }
    
    val fileMonitor = new FileMonitor(executionPollingFrequencyInHz, maxWaitTimeForOutputs)

    new SqueuePoller(invoker, actualExecutable, fileMonitor)
  }
      
  private[slurm] def makeTokens(actualExecutable: String)(drmTaskIds: Iterable[DrmTaskId]): Seq[String] = {
    import DrmSystem.Slurm.{ format => taskIdToString }
    
    Seq(
      actualExecutable,
      //no header, just data lines
      "-h", 
      //ask for <job_id>_<index> and abbreviated status; 
      //output will be '|' delimited without a '|' at the end
      "-o", "%i|%t", 
      //specify job/task ids we're looking for, comma-separated, in <job_id>_<index> format
      "-j", 
      
      drmTaskIds.iterator.map(taskIdToString).mkString(","))
  }
      
  private[slurm] def parseDataLine(line: String): Try[PollResult] = {
    val parts = line.trim.split("\\|").filter(_.nonEmpty)
    
    parts match {
      case Array(jobIdPart, statusPart, _ @ _*) => {
        import SlurmStatus._
        
        for {
          drmTaskId <- parseDrmTaskId(jobIdPart)
          status <- (tryFromShortName(statusPart) orElse tryFromFullName(statusPart))
        } yield drmTaskId -> status.drmStatus
      }
      case _ => Tries.failure(s"Couldn't parse data line: '${line}' (split into ${parts})")
    }
  }
  
  private object Regexes {
    val jobAndTaskIndex = "^(\\d+)_(\\d+)$".r
    val jobId = "^(\\d+)$".r
  }
  
  private[slurm] def parseDrmTaskId(s: String): Try[DrmTaskId] = s.trim match {
    case Regexes.jobAndTaskIndex(jobId, taskIndex) => Success(DrmTaskId(jobId, taskIndex.toInt))
    //case Regexes.jobId(jobId) => Success(DrmTaskId(jobId, 0)) //TODO
    case _ => Tries.failure(s"Couldn't parse DrmTaskId from '$s'")
  }
}