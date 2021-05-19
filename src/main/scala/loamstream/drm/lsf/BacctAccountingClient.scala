package loamstream.drm.lsf

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import loamstream.conf.LsfConfig
import loamstream.drm.AccountingClient
import loamstream.drm.DrmTaskId
import loamstream.drm.Queue
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.jobs.TerminationReason
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Options
import loamstream.util.Tries
import monix.execution.Scheduler
import monix.eval.Task


/**
 * @author clint
 * Apr 18, 2019
 */
final class BacctAccountingClient(
    bacctInvoker: CommandInvoker.Async[DrmTaskId])
    (implicit ec: ExecutionContext) extends AccountingClient with Loggable {

  import BacctAccountingClient._

  override def getResourceUsage(taskId: DrmTaskId): Task[LsfResources] = {
    getBacctOutputFor(taskId).flatMap(output => Task.fromTry(toResources(output)))
  }
  
  override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = {
    getBacctOutputFor(taskId).map(toTerminationReason)
  }
  
  private def getBacctOutputFor(taskId: DrmTaskId): Task[Seq[String]] = {
    bacctInvoker(taskId).map(_.stdout.map(_.trim))
  }
    
  private def toTerminationReason(bacctOutput: Seq[String]): Option[TerminationReason] = {
    bacctOutput.collectFirst { case Regexes.termReason(r) => r }.map(parseTerminationReason)
  }
  
  /*
   * Documentation on bacct's output format:
   * https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_command_ref/bacct.1.html
   */
  private def toResources(rawBacctOutput: Seq[String]): Try[LsfResources] = {
    /*
     * NB: The 10.x version of `bacct` drops the "unformatted" output option, and consequently is fairly eager 
     * about inserting line breaks and odd amounts of indentation on broken lines.  For example, what the LSF 
     * 9.x bacct would render as
     * 
     * A very, very, very long line with <embedded fields and things like that>
     * 
     * is rendered in 10.x as something like (narrow lines to make the point)
     * 
     * A very, very, ve
     *   ry long line w
     *   ith <embedded 
     *   fields and thi
     *   ngs like that>
     *   
     * The code below takes the sequence of lines from bacct, joins them with a conspicuous delimiter, and
     * then removes the delimiter and any whitespace immediately following it. This gets rid of the line breaks
     * and the added indentation, allowing fields delimited by angle brackets, etc, to be retrieved more easily.
     */

    //Use a delimiter that won't occur in `bacct`'s output, so we can find where line-breaks used to be.
    val delim = "%%%%%%%%%%%%"
    
    val joinedBacctOutput = rawBacctOutput.mkString(delim).replaceAll(s"${delim}\\s*", "")
    
    def extract(regex: Regex): Option[String] = joinedBacctOutput match {
      case regex(s) => Option(s.trim)
      case _ => None
    }
    
    val node = extract(Regexes.node)
    
    val queue = extract(Regexes.queue).map(Queue(_))
    
    val dataLineOpt = rawBacctOutput.sliding(2).collectFirst {
      case Seq(firstLine, nextLine) if isHeaderLine(firstLine) => nextLine.trim
    }
    
    val dataLineAttempt = Options.toTry(dataLineOpt)(s"Couldn't find data line in bacct output: '$rawBacctOutput'")
    
    dataLineAttempt.flatMap(parseDataLine(rawBacctOutput, node, queue))
  }
}

object BacctAccountingClient {
  /**
   * Make a QacctAccountingClient that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(
      lsfConfig: LsfConfig, 
      scheduler: Scheduler,
      binaryName: String = "bacct")(implicit ec: ExecutionContext): BacctAccountingClient = {
    new BacctAccountingClient(BacctInvoker.useActualBinary(lsfConfig.maxBacctRetries, binaryName, scheduler))
  }
  
  //"Data lines" look like this:
  //CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
  //0.02        0              0     exit         0.0000     0M      0M
  def parseDataLine(
      rawBacctOutput: Seq[String], 
      node: Option[String], 
      queue: Option[Queue])(line: String): Try[LsfResources] = {
    
    val parts = line.trim.split("\\s+")
      
    def tryToGet(i: Int, message: => String): Try[String] = {
      if(parts.isDefinedAt(i)) Success(parts(i)) else Tries.failure(message)
    }
    
    val memIndex = 5 //scalastyle:ignore magic.number
    val cpuTimeIndex = 0
    
    for {
      memory <- tryToGet(memIndex, s"Couldn't parse memory usage from bacct line '$line'").flatMap(parseMemory(line))
      cpuTime <- tryToGet(cpuTimeIndex, s"Couldn't parse cpu time usage from bacct line '$line'").flatMap(parseCpuTime)
      startTime <- parseStartTime(rawBacctOutput)
      endTime <- parseEndTime(rawBacctOutput)
      
    } yield {
      LsfResources(
        memory = memory,
        cpuTime = cpuTime,
        node = node,
        queue = queue,
        startTime = startTime,
        endTime = endTime,
        raw = Some(rawBacctOutput.mkString("\n")))
    }
  }
  
  private def isHeaderLine(s: String): Boolean = s.trim.startsWith("CPU_T")
  
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
  
  private[lsf] def parseStartTime(rawBacctOutput: Seq[String]): Try[LocalDateTime] = {
    parseTimestamp(rawBacctOutput)(Regexes.startTime, "start")
  }
  
  private[lsf] def parseEndTime(rawBacctOutput: Seq[String]): Try[LocalDateTime] = {
    parseTimestamp(rawBacctOutput)(Regexes.endTime, "end")
  }
  
  private def parseTimestamp(rawBacctOutput: Seq[String])(regex: Regex, fieldType: String): Try[LocalDateTime] = {
    val dateOpt = rawBacctOutput.collectFirst { case regex(v) => v }
    
    def failureMessage = s"Couldn't parse $fieldType timestamp from bacct output '$rawBacctOutput'"
    
    val dateStringAttempt = Options.toTry(dateOpt)(failureMessage)
    
    val (primaryFormatter, alternateFormatter) = dateFormatters 
    
    def parseLocalDateTime(formatter: DateTimeFormatter): Try[LocalDateTime] = {
      dateStringAttempt.map(ds => formatter.parse(ds, LocalDateTime.from))
    }
    
    parseLocalDateTime(primaryFormatter).orElse(parseLocalDateTime(alternateFormatter))
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
    val queue = "(?s).*Queue <(.+?)>.*".r
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
