package loamstream.util

import java.time.temporal.Temporal
import java.time.Instant

/**
 * @author clint
 * date: Aug 11, 2016
 */
object TimeUtils extends Loggable {
  def startAndEndTime[A](block: => A): (A, (Instant, Instant)) = {
    val start = Instant.now
    
    val result = block
      
    val end = Instant.now

    (result, (start, end))
  }
  
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
