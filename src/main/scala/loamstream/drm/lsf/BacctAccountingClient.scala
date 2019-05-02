package loamstream.drm.lsf

import scala.util.Try

import loamstream.model.execute.Resources.LsfResources
import loamstream.util.Tries
import loamstream.util.Options
import scala.util.Success
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import java.time.Instant
import loamstream.drm.Queue
import scala.util.matching.Regex
import java.time.format.DateTimeFormatterBuilder
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.time.ZoneId
import loamstream.util.RetryingCommandInvoker
import loamstream.drm.AccountingClient
import loamstream.util.Loggable
import loamstream.conf.LsfConfig

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
  
  private def getBacctOutputFor(jobId: String): Try[Seq[String]] = bacctInvoker(jobId).map(_.stdout)
    
  private def toResources(bacctOutput: Seq[String]): Try[LsfResources] = {
    val mungedBacctOutput = bacctOutput.map(_.trim)
    
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
    
    for {
      memory <- tryToGet(5, s"Couldn't parse memory usage from bacct line '$line'").flatMap(parseMemory(line))
      cpuTime <- tryToGet(0, s"Couldn't parse cpu time usage from bacct line '$line'").flatMap(parseCpuTime)
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
   * manifest on runs that cross year boundaries are not something that would be fun to debug in fome far-off future.
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
    
    (makeDateTimeFormatter("EEE MMM dd HH:mm:ss"), makeDateTimeFormatter("EEE MMM  d HH:mm:ss"))
  }
  
  private[lsf] object Regexes {
    val node = ".*[Dd]ispatched to <(.+?)>.*".r
    val queue = ".*Queue <(.+?)>.*".r
    //Thu Apr 18 22:32:01: Dispatched to <ebi6-054>
    val startTime = "(.*):.*[Dd]ispatched\\sto.*".r
    //Thu Apr 18 22:32:01: Completed <exit>.
    val endTime = "(.*):\\s+Completed.*".r
    val memory = "^(.+)[MmGg]$".r
  }
}
