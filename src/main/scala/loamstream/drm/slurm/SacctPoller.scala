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
import SacctPoller.Params
import loamstream.conf.ExecutionConfig
import monix.execution.Scheduler
import loamstream.util.RunResults
import loamstream.util.Tries
import loamstream.util.Options
import scala.util.Success
import loamstream.drm.DrmSystem

/**
 * @author clint
 * May 18, 2021
 *
 * Asynchronously inquire about the status of one or more jobs by invoking `sacct`/
 *
 * @param taskIds the ids of the tasks to inquire about
 * @return a map of task ids to attempts at that task's status
 */
final class SacctPoller private[slurm] (
    pollingFn: CommandInvoker.Async[Params],
    commandName: String = "sacct") extends RateLimitedPoller[Params](commandName, pollingFn) {

  protected override def toParams(oracle: DrmJobOracle)(drmTaskIds: Iterable[DrmTaskId]): Params = drmTaskIds
  
  import SacctPoller.PollResult
  
  override protected def getExitCodes(
      oracle: DrmJobOracle)
     (runResults: RunResults.Successful, 
      idsToLookFor: Set[DrmTaskId]): Observable[PollResult] = Observable.empty
  
  override protected def getStatusesByTaskId(
      idsWereLookingFor: Iterable[DrmTaskId])
     (runResults: RunResults.Successful): PollResultsForInvocation = {
    
    val outputLines = runResults.stdout
    
    println(s"%%%%%%%%%%% ${runResults.stdout}")
    println(s"%%%%%%%%%%% ${runResults.stderr}")
    
    val statusAttemptsById = outputLines.iterator.map(SacctPoller.parseDataLine).map { //TODO 
      case Success((drmTaskId, drmStatus)) => (drmTaskId, Success(drmStatus))
    }.toMap
    
    PollResultsForInvocation(runResults, statusAttemptsById)
  }
}

object SacctPoller extends RateLimitedPoller.Companion[Iterable[DrmTaskId], SacctPoller] with Loggable {
  def fromExecutable(
      actualExecutable: String,
      pollingFrequencyInHz: Double,
      executionConfig: ExecutionConfig,
      scheduler: Scheduler): SacctPoller = {
    
    val invoker = commandInvoker(
        pollingFrequencyInHz, 
        actualExecutable, 
        makeTokens(actualExecutable)(_))(scheduler, this) 
    
    new SacctPoller(invoker, actualExecutable)
  }
      
  private[slurm] def makeTokens(actualExecutable: String)(drmTaskIds: Iterable[DrmTaskId]): Seq[String] = {
    import DrmSystem.Slurm.{ format => taskIdToString }
    
    Seq(
      actualExecutable,
      "-b", //brief output: job/task id, state, exit code
      "-n", //no header, just data lines
      "-P", //output will be '|' delimited without a '|' at the end
      "-j", //specify job/task ids we're looking for, comma-separated
      
      drmTaskIds.iterator.map(taskIdToString).mkString(","))
  }
      
  private[slurm] def parseDataLine(line: String): Try[PollResult] = {
    val parts = line.trim.split("\\|").filter(_.nonEmpty)
    
    parts match {
      case Array(jobIdPart, statusPart, _) => {
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
    val jobAndTaskIndex = "^(\\d+)\\.(\\d+)$".r
    val jobId = "^(\\d+)$".r
  }
  
  private[slurm] def parseDrmTaskId(s: String): Try[DrmTaskId] = s.trim match {
    case Regexes.jobAndTaskIndex(jobId, taskIndex) => Success(DrmTaskId(jobId, taskIndex.toInt))
    case Regexes.jobId(jobId) => Success(DrmTaskId(jobId, 0)) //TODO
    case _ => Tries.failure(s"Couldn't parse DrmTaskId from '$s'")
  }
}