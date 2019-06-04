package loamstream.drm.lsf

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import loamstream.conf.LsfConfig
import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.Loggable
import loamstream.util.Options
import loamstream.util.RetryingCommandInvoker
import loamstream.util.Tries
import loamstream.model.jobs.TerminationReason

/**
 * @author clint
 * Apr 18, 2019
 */
final class BacctAccountingClient(
    bacctInvoker: RetryingCommandInvoker[String]) extends AccountingClient with Loggable {

  import BacctAccountingClient._

  override def getResourceUsage(jobId: String): Try[LsfResources] = {
    getBacctOutputFor(jobId).flatMap(toResources)
  }
  
  override def getTerminationReason(jobId: String): Try[Option[TerminationReason]] = {
    getBacctOutputFor(jobId).map(toTerminationReason)
  }
  
  private def getBacctOutputFor(jobId: String): Try[Seq[String]] = bacctInvoker(jobId).map(_.stdout.map(_.trim))
    
  private def toTerminationReason(bacctOutput: Seq[String]): Option[TerminationReason] = {
    bacctOutput.collectFirst { case Regexes.termReason(r) => r }.map(parseTerminationReason)
  }
  
  private def toResources(mungedBacctOutput: Seq[String]): Try[LsfResources] = {
    /*
     * Documentation on bacct's output format:
     * https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_command_ref/bacct.1.html
     */
    val node = mungedBacctOutput.collectFirst { case Regexes.node(n) => n }
    
    val queue = mungedBacctOutput.collectFirst { case Regexes.queue(q) => q }.map(Queue(_))
    
    val dataLineOpt = mungedBacctOutput.sliding(2).collectFirst {
      case Seq(firstLine, nextLine) if isHeaderLine(firstLine) => nextLine.trim
    }
    
    val dataLineAttempt = Options.toTry(dataLineOpt)(s"Couldn't find data line in bacct output: '$mungedBacctOutput'")
    
    dataLineAttempt.flatMap(parseDataLine(mungedBacctOutput, node, queue))
  }
}

object BacctAccountingClient {
  /**
   * Make a QacctAccountingClient that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(lsfConfig: LsfConfig, binaryName: String = "bacct"): BacctAccountingClient = {
    new BacctAccountingClient(BacctInvoker.useActualBinary(lsfConfig.maxBacctRetries, binaryName))
  }
  
  //"Data lines" look like this:
  //CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
  //0.02        0              0     exit         0.0000     0M      0M
  def parseDataLine(
      mungedBacctOutput: Seq[String], 
      node: Option[String], 
      queue: Option[Queue])(line: String): Try[LsfResources] = {
    
    val parts = line.split("\\s+")
      
    def tryToGet(i: Int, message: => String): Try[String] = {
      if(parts.isDefinedAt(i)) Success(parts(i)) else Tries.failure(message)
    }
    
    val memIndex = 5 //scalastyle:ignore magic.number
    val cpuTimeIndex = 0
    
    for {
      memory <- tryToGet(memIndex, s"Couldn't parse memory usage from bacct line '$line'").flatMap(parseMemory(line))
      cpuTime <- tryToGet(cpuTimeIndex, s"Couldn't parse cpu time usage from bacct line '$line'").flatMap(parseCpuTime)
      startTime <- parseStartTime(mungedBacctOutput)
      endTime <- parseEndTime(mungedBacctOutput)
      
    } yield {
      LsfResources(
        memory = memory,
        cpuTime = cpuTime,
        node = node,
        queue = queue,
        startTime = startTime,
        endTime = endTime)
    }
  }
  
  private def isHeaderLine(s: String): Boolean = s.startsWith("CPU_T")
  
  //NB: assume megabytes or gigabytes; the LSF documentation doesn't describe what units are possible. :(
  private[lsf] def parseMemory(line: String)(s: String): Try[Memory] = {
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
  
  private[lsf] def parseStartTime(mungedBacctOutput: Seq[String]): Try[Instant] = {
    parseTimestamp(mungedBacctOutput)(Regexes.startTime, "start")
  }
  
  private[lsf] def parseEndTime(mungedBacctOutput: Seq[String]): Try[Instant] = {
    parseTimestamp(mungedBacctOutput)(Regexes.endTime, "end")
  }
  
  private def parseTimestamp(mungedBacctOutput: Seq[String])(regex: Regex, fieldType: String): Try[Instant] = {
    val dateOpt = mungedBacctOutput.collectFirst { case regex(v) => v }
    
    def failureMessage = s"Couldn't parse $fieldType timestamp from bacct output '$mungedBacctOutput'"
    
    val dateStringAttempt = Options.toTry(dateOpt)(failureMessage)
    
    val (primaryFormatter, alternateFormatter) = dateFormatters 
    
    def parseInstant(formatter: DateTimeFormatter): Try[Instant] = {
      dateStringAttempt.map(ds => formatter.parse(ds, Instant.from))
    }
    
    parseInstant(primaryFormatter).orElse(parseInstant(alternateFormatter))
  }
  
  /*
   * Parses dates of the form: Thu Apr 18 22:32:01
   * Since that date format doesn't include a year or time zone, we have to provide default values for those.  This 
   * code asks the runtime for them, each time a formatter is made.  (The formatter takes all its parameters by-value.)
   * Consequently, this formatter needs to be a def. It wouldn't be so bad to cache the formatter and assume the year 
   * is the same throughout a run of LS, but I don't think that will save very much cpu time, and date bugs that only
   * manifest on runs that cross year boundaries are not something that would be fun to debug in some far-off future.
   *  -Clint Apr 22, 2019
   */
  private def dateFormatters: (DateTimeFormatter, DateTimeFormatter) = {
    val systemTimeZoneId = ZoneId.of(java.util.TimeZone.getDefault.getID)

    val currentYear: Int = Instant.now.atZone(systemTimeZoneId).get(ChronoField.YEAR)

    def makeDateTimeFormatter(pattern: String) = {
      (new DateTimeFormatterBuilder)
        .appendPattern(pattern)
        .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
        .parseDefaulting(ChronoField.YEAR, currentYear)
        .toFormatter
        .withZone(systemTimeZoneId)
    }
    
    //NB: Return two formatters, differing in the number of characters the day-of-month part will take, and 
    //associated padding.  To the best of my knowledge, there is no way to say something like "one or more 
    //day-of-month digits", or "one or two day-of-month digits".  Instead, we return two formatters, so that 
    //the second may be tried when parsing if the first one doesn't work.
    (makeDateTimeFormatter("EEE MMM dd HH:mm:ss"), makeDateTimeFormatter("EEE MMM  d HH:mm:ss"))
  }
  
  private[lsf] object Regexes {
    val node = ".*[Dd]ispatched to <(.+?)>.*".r
    val queue = ".*Queue <(.+?)>.*".r
    //Thu Apr 18 22:32:01: Dispatched to <ebi6-054>
    val startTime = "(.*):.*[Dd]ispatched\\sto.*".r
    //Thu Apr 18 22:32:01: Completed <exit>.
    val endTime = "(.*):\\s+Completed.*".r
    //Mon May  6 22:58:03: Completed <exit>; TERM_RUNLIMIT: job killed after reaching LSF run time limit.
    val termReason = ".*:\\s+Completed.*;\\s+(TERM_.*):.*".r
    //MEM SWAP
    //3M  60M
    val memory = "^(.+)[MmGg]$".r
  }
  
  //See https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_command_ref/bacct.1.html
  private[lsf] def parseTerminationReason(lsfReason: String): TerminationReason = lsfReason.trim.toUpperCase match {
    //Job was killed after it reached LSF CPU usage limit (12)
    case "TERM_CPULIMIT" => TerminationReason.CpuTime
    
    //Job was killed by owner (14)
    //Job was killed by owner without time for cleanup (8)
    case ("TERM_OWNER" | "TERM_FORCE_OWNER") => TerminationReason.UserRequested
    
    //Job was killed after it reached LSF memory usage limit (16)
    //Job was killed after it reached LSF swap usage limit (20)
    case ("TERM_MEMLIMIT" | "TERM_SWAP") => TerminationReason.Memory 
    
    //Job was killed after it reached LSF runtime limit (5)
    case "TERM_RUNLIMIT" => TerminationReason.RunTime
    
    //LSF cannot determine a termination reason. 0 is logged but "TERM_UNKNOWN is not displayed (0)
    case "TERM_UNKNOWN" => TerminationReason.Unknown
    
    case _ => TerminationReason.Unknown
  }
}
