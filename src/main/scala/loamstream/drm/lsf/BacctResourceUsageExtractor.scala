package loamstream.drm.lsf

import scala.util.Try

import loamstream.drm.ResourceUsageExtractor
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

/**
 * @author clint
 * Apr 18, 2019
 */
object BacctResourceUsageExtractor extends ResourceUsageExtractor[Seq[String]] {
  override def toResources(bacctOutput: Seq[String]): Try[LsfResources] = {
    //Dispatched to <ebi6-054>
    
    val node = bacctOutput.collectFirst { case Regexes.node(n) => n }
    
    val queue = bacctOutput.collectFirst { case Regexes.queue(q) => q }.map(Queue(_))
    
    def isHeaderLine(s: String): Boolean = s.trim.startsWith("CPU_T")
    
    val dataLineOpt = bacctOutput.sliding(2).collectFirst {
      case Seq(firstLine, nextLine) if isHeaderLine(firstLine) => nextLine.trim
    }
    
    val dataLineAttempt = Options.toTry(dataLineOpt)(s"Couldn't find data line in bacct output: '$bacctOutput'")
    
    //CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
    //0.02        0              0     exit         0.0000     0M      0M
    
    def parseDataLine(line: String): Try[LsfResources] = {
      val parts = line.split("\\s+")
        
      def tryToGet(i: Int, message: => String): Try[String] = {
        if(parts.isDefinedAt(i)) Success(parts(i)) else Tries.failure(message)
      }

      //NB: assume megabytes :(
      def parseMemory(s: String): Try[Memory] = s match {
        case Regexes.memory(howMuch) => Try(Memory.inMb(howMuch.toDouble))
        case _ => Tries.failure(s"Couldn't parse memory information from '$s', part of line '$line'")
      }
      
      def parseCpuTime(s: String): Try[CpuTime] = Try(CpuTime.inSeconds(s.toDouble))
        
      def parseTime(regex: Regex, fieldType: String): Try[Instant] = {
        println(s"%%%%%%%%%%%%% parsing time line: ")
        
        val dateOpt = bacctOutput.iterator.map(_.trim).collectFirst { case regex(v) => v }
        
        def failureMessage = s"Couldn't parse $fieldType timestamp from bacct output '$bacctOutput'"
        
        val dateStringAttempt = Options.toTry(dateOpt)(failureMessage)
        
        dateStringAttempt.map(ds => dateFormatter.parse(ds, Instant.from))
      }
      
      for {
        memory <- tryToGet(6, s"Couldn't parse memory usage from bacct line '$line'").flatMap(parseMemory)
        cpuTime <- tryToGet(0, s"Couldn't parse cpu time usage from bacct line '$line'").flatMap(parseCpuTime)
        startTime <- parseTime(Regexes.startTime, "start")
        endTime <- parseTime(Regexes.endTime, "end")
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
    
    for {
      dataLine <- dataLineAttempt
      resources <- parseDataLine(dataLine)
    } yield resources
  }

  private def currentYear: Int = Instant.now.atZone(ZoneId.of("UTC")).get(ChronoField.YEAR)
  
  //Thu Apr 18 22:32:01
  private def dateFormatter: DateTimeFormatter = (new DateTimeFormatterBuilder)
    .appendPattern("EEE MMM dd HH:mm:ss")
    .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
    .parseDefaulting(ChronoField.YEAR, currentYear)
    .toFormatter
    .withZone(ZoneId.of("UTC"))
  
  private object Regexes {
    val node = ".*Dispatched to <(.+?)>.*".r
    val queue = ".*Queue <(.+?)>.*".r
    //Thu Apr 18 22:32:01: Dispatched to <ebi6-054>
    val startTime = "(.*):\\s+Dispatched\\sto.*".r
    //Thu Apr 18 22:32:01: Completed <exit>.
    val endTime = "(.*):\\s+Completed.*".r
    val memory = "^(\\d+)M$".r
  }
}
