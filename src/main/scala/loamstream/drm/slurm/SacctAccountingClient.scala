package loamstream.drm.slurm

import loamstream.drm.AccountingClient
import loamstream.drm.DrmTaskId
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import monix.eval.Task
import loamstream.model.execute.Resources.SlurmResources
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import scala.util.Try
import loamstream.util.Regexes
import loamstream.util.Options
import java.awt.Queue
import scala.util.matching.Regex
import scala.util.Success
import loamstream.util.Tries
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import scala.util.Failure
import loamstream.conf.SlurmConfig
import monix.execution.Scheduler
import loamstream.util.RunResults

/**
 * @author clint
 * 
 * May 18, 2021
 */
final class SacctAccountingClient(
    sacctInvoker: CommandInvoker.Async[DrmTaskId]) extends AccountingClient with Loggable {
  
  import SacctAccountingClient._

  override def getResourceUsage(taskId: DrmTaskId): Task[SlurmResources] = {
    getBacctOutputFor(taskId).flatMap(output => Task.fromTry(toResources(output)))
  }
  
  override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = {
    getBacctOutputFor(taskId).map(toTerminationReason)
  }
  
  private def getBacctOutputFor(taskId: DrmTaskId): Task[Seq[String]] = {
    sacctInvoker(taskId).map(_.stdout.map(_.trim))
  }
    
  private def toTerminationReason(sacctOutput: Seq[String]): Option[TerminationReason] = {
    //sacctOutput.collectFirst { case Regexes.termReason(r) => r }.map(parseTerminationReason)
    
    //TODO: Does SLURM report this?
    None
  }
  
  /*
   * Documentation on sacct's output format:
   * https://slurm.schedmd.com/sacct.html
   */
  private def toResources(rawSacctOutput: Seq[String]): Try[SlurmResources] = {

    val dataLineAttempt = {
      Options.toTry(rawSacctOutput.headOption)(s"Couldn't find data line in sacct output: '$rawSacctOutput'")
    }
    
    dataLineAttempt.flatMap(parseDataLine(rawSacctOutput))
  }
}

object SacctAccountingClient {
  /**
   * Make a SacctAccountingClient that will retrieve job metadata by running some executable, by default, `sacct`.
   */
  def useActualBinary(
      slurmConfig: SlurmConfig, 
      scheduler: Scheduler,
      binaryName: String = "bacct"): SacctAccountingClient = {
    new SacctAccountingClient(
      SacctInvoker.useActualBinary(
        slurmConfig.maxSacctRetries, 
        binaryName, 
        scheduler,
        isSuccess = RunResults.SuccessPredicate.zeroIsSuccess))
  }
  
  //"Data lines" look like this:
  //MaxRSS|CPUTime|Start|End|NodeList
  //------------------------------------  
  //|00:00:00|2021-06-03T21:16:57|2021-06-03T21:17:02|some-machine
  //Everything above, and including, the `-----...` line is added for clarity, but is omitted by sacct when
  // -n/--noheader is used.
  def parseDataLine(
      rawBacctOutput: Seq[String])(line: String): Try[SlurmResources] = {
    
    val parts = line.trim.split("|")
      
    def tryToGet(i: Int, message: => String): Try[String] = {
      if(parts.isDefinedAt(i)) Success(parts(i)) else Tries.failure(message)
    }
    
    val memIndex = 0 //scalastyle:ignore magic.number
    val cpuTimeIndex = 1 //scalastyle:ignore magic.number
    val startTimeIndex = 2 //scalastyle:ignore magic.number
    val endTimeIndex = 3 //scalastyle:ignore magic.number
    val nodeIndex = 4 //scalastyle:ignore magic.number
    
    def msg(field: String) = s"Couldn't parse $field from sacct line '$line'"
    
    for {
      memory <- tryToGet(memIndex, msg("memory usage")).flatMap(parseMemory(line))
      cpuTime <- tryToGet(cpuTimeIndex, msg("cpu time usage")).flatMap(parseCpuTime)
      startTime <- tryToGet(startTimeIndex, msg("start time")).flatMap(parseStartTime(rawBacctOutput))
      endTime <- tryToGet(startTimeIndex, msg("end time")).flatMap(parseEndTime(rawBacctOutput))
      node <- tryToGet(nodeIndex, msg("execution node"))
    } yield {
      SlurmResources(
        memory = memory,
        cpuTime = cpuTime,
        node = Option(node),
        queue = None, //TODO: Does SLURM have a notion of queues?
        startTime = startTime,
        endTime = endTime,
        raw = Some(rawBacctOutput.mkString("\n")))
    }
  }
  
  private[slurm] def parseMemory(line: String)(s: String): Try[Memory] = {
    val toMemory: Double => Memory = s.last.toUpper match {
      case 'M' => Memory.inMb
      case 'G' => Memory.inGb
    }
    
    s match {
      case Regexes.memory(howMuch) => Try(toMemory(howMuch.toDouble))
      case _ => Tries.failure(s"Couldn't parse memory information from '$s', part of line '$line'")
    }
  }
  
  /*
   * Cpu time is reported in seconds.  See:
   * https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_command_ref/bacct.1.html
   */
  private def parseCpuTime(s: String): Try[CpuTime] = Try(CpuTime.inSeconds(s.toDouble))
  
  private[slurm] def parseStartTime(rawBacctOutput: Seq[String])(dateString: String): Try[LocalDateTime] = {
    parseTimestamp(rawBacctOutput)("start", dateString)
  }
  
  private[slurm] def parseEndTime(rawBacctOutput: Seq[String])(dateString: String): Try[LocalDateTime] = {
    parseTimestamp(rawBacctOutput)("end", dateString)
  }
  
  private def parseTimestamp(rawBacctOutput: Seq[String])(fieldType: String, dateString: String): Try[LocalDateTime] = {
    def failureMessage = s"Couldn't parse $fieldType timestamp from bacct output '$rawBacctOutput'"
    
    Try(LocalDateTime.parse(dateString)).recoverWith { case cause => Failure(new Exception(failureMessage, cause)) }
  }
  
  private[slurm] object Regexes {
    val termReason = ".*:\\s+Completed.*;\\s+(TERM_.*):.*".r
    //MEM SWAP
    //3M  60M
    val memory = "^(.+)[MmGg]$".r
  }
}