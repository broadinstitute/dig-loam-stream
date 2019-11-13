package loamstream.util

import java.time.temporal.Temporal
import java.time.Instant
import scala.util.Try
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZoneId

/**
 * @author clint
 * date: Aug 11, 2016
 */
object TimeUtils extends Loggable {
  def startAndEndTime[A](block: => A): (Try[A], (LocalDateTime, LocalDateTime)) = {
    val start = LocalDateTime.now
    
    val result = Try(block)
      
    val end = LocalDateTime.now

    (result, (start, end))
  }
  
  def elapsed[A](block: => A): (Try[A], Long) = {
    val (attempt, (start, end)) = startAndEndTime(block)
    
    (attempt, toEpochMilli(end) - toEpochMilli(start))
  }
  
  private val systemZoneId = ZoneId.systemDefault
  
  def toEpochMilli(ldt: LocalDateTime): Long = ldt.atZone(systemZoneId).toInstant.toEpochMilli
  
  def time[A](message: => String, doPrint: String => Any = trace(_))(block: => A): A = {
    val start = System.currentTimeMillis

    try { block }
    finally {
      val end = System.currentTimeMillis

      val elapsed = end - start

      doPrint(s"$message took $elapsed ms")
    }
  }

  object Implicits {
    implicit final class TemporalOps[T <: Temporal with Comparable[T]](val lhs: T) extends AnyVal {
      private def compareTo(rhs: T): Int = lhs.compareTo(rhs)

      def <(rhs: T): Boolean = compareTo(rhs) <  0

      def >(rhs: T): Boolean = compareTo(rhs) >  0

      def <=(rhs: T): Boolean = compareTo(rhs) <= 0

      def >=(rhs: T): Boolean = compareTo(rhs) >= 0
    }
  }
}
