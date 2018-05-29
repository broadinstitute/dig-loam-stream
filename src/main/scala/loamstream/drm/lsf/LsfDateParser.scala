package loamstream.drm.lsf

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.time.LocalDateTime
import loamstream.util.ValueBox
import scala.util.Try
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.ZoneOffset

/**
 * @author clint
 * May 22, 2018
 */
object LsfDateParser {
  private[lsf] def toInstant(lsfRep: String): Option[Instant] = {

    //NB: LSF's default date format (it's a pain to change it) looks like:
    //May 21 23:44
    //So we need to fill in some implied or omitted fields: current year, seconds, milliseconds
    
    val attempt = Try {
      
      //TODO: Cache formatters by year if building one is expensive 
      val formatter = {
        val currentYear = LocalDateTime.now.getYear
        
        (new DateTimeFormatterBuilder).appendPattern("MMM dd HH:mm").
            parseDefaulting(ChronoField.YEAR, currentYear).
            parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).
            parseDefaulting(ChronoField.MILLI_OF_SECOND, 0).
            toFormatter
      }
      
      val mungedLsfRep = if(lsfRep.endsWith("L")) lsfRep.dropRight(1).trim else lsfRep
      
      formatter.parse(mungedLsfRep, LocalDateTime.from(_)).toInstant(ZoneOffset.UTC)
    }
    
    attempt.toOption
  }
}
