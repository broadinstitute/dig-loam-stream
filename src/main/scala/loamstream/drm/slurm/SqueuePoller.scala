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
import scala.util.Failure

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
     (runResults: RunResults.Completed): PollResultsForInvocation = {
    
    val outputLines = runResults.stdout
    
    val statusAttemptsById = outputLines.iterator.map(SqueuePoller.parseDataLine).flatMap { //TODO 
      case Success(pollResults) => pollResults.map { case (drmTaskId, drmStatus) => (drmTaskId, Success(drmStatus)) }
      case Failure(e) => throw new Exception(s"Error parsing ${commandName} output: looking for ids: ${idsWereLookingFor}; runResults: ${runResults}", e)
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
      
  private[slurm] def parseDataLine(line: String): Try[Iterable[PollResult]] = {
    val parts = line.trim.split("\\|").filter(_.nonEmpty)
    
    parts match {
      case Array(jobIdPart, statusPart, _ @ _*) => {
        import SlurmStatus._
        
        for {
          drmTaskIds <- SlurmDrmTaskId.parseDrmTaskIds(jobIdPart)
          status <- (tryFromShortName(statusPart) orElse tryFromFullName(statusPart))
        } yield {
          trace(s"Got SLURM status '${status}' (raw: '${statusPart}') for task IDs ${drmTaskIds}")

          if(status == SlurmStatus.Completing) {
            val msg = {
              s"Got SLURM Status ${status.fullName} for task id(s) ${drmTaskIds}.  LS will treat it as a " +
              "success, but it may indicate problems.  See https://slurm.schedmd.com/troubleshoot.html#completing"
            }

            warn(msg)
          }

          //TODO: Does Slurm really report multiple jobs with the same status?
          drmTaskIds.map(_ -> status.drmStatus)
        }
      }
      case _ => Tries.failure(s"Couldn't parse data line: '${line}' (split into ${parts})")
    }
  }
  
  
}