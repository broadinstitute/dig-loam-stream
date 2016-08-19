package loamstream.util

import java.time.temporal.Temporal

/**
 * @author clint
 * date: Aug 11, 2016
 */
object TimeEnrichments {
    
  implicit final class TemporalOps[T <: Temporal with Comparable[T]](val lhs: T) extends AnyVal {
    private def compareTo(rhs: T): Int = lhs.compareTo(rhs)
    
    def <(rhs: T): Boolean = compareTo(rhs) <  0

    def >(rhs: T): Boolean = compareTo(rhs) >  0

    def <=(rhs: T): Boolean = compareTo(rhs) <= 0

    def >=(rhs: T): Boolean = compareTo(rhs) >= 0
  }
}