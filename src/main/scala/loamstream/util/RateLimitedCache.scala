package loamstream.util

import java.time.LocalDateTime
import RateLimitedCache.State
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * @author clint
 * Sep 8, 2020
 * 
 * A wrapper around a function that caches the result produced by that function.  When apply() is called,
 * the wrapped function is called only if it's never been called, or if more than `maxAge` has elapsed
 * since it was last invoked.  Otherwise, the cached value is returned.    
 */
final class RateLimitedCache[P, A](delegate: P => Try[A], maxAge: Duration) extends (P => Try[A]) {
  private val stateBox: ValueBox[State[A]] = ValueBox(State.Initial)
  
  def lastAccessed: Long = stateBox.get(_.lastAccessed)
  
  def lastModified: Long = stateBox.get(_.lastModified)
  
  override def apply(params: P): Try[A] = stateBox.getAndUpdate { state =>
    val shouldRun: Boolean = state match {
      case State.Initial => true
      case s @ State.WithValue(_, _, _) => s.hasBeenLongerThan(maxAge)
    }
    
    if(shouldRun) {
      val valueAttempt: Try[A] = delegate(params)
      
      (State(valueAttempt), valueAttempt)
    } else {
      (state.updateLastAccessed, state.lastValue)
    }
  }
}

object RateLimitedCache {
  def withMaxAge[P, A](maxAge: Duration)(op: => Try[A]): RateLimitedCache[P, A] = new RateLimitedCache(_ => op, maxAge)
  
  def withMaxAge[P, A](maxAge: Duration)(op: P => Try[A]): RateLimitedCache[P, A] = new RateLimitedCache(op(_), maxAge)
  
  sealed trait State[+A] {
    def lastModified: Long
    
    def lastAccessed: Long
    
    def updateLastAccessed: State[A]
    
    def lastValue: Try[A]
  }
  
  object State {
    case object Initial extends State[Nothing] {
      override def lastModified: Long = Long.MinValue
    
      override def lastAccessed: Long = Long.MinValue
      
      override def updateLastAccessed: State[Nothing] = sys.error("Can't access Initial state")
      
      override def lastValue: Nothing = sys.error("No last value")
    }

    final case class WithValue[A](lastModified: Long, lastAccessed: Long, lastValue: Try[A]) extends State[A] {
      def hasBeenLongerThan(duration: Duration, now: Long = System.currentTimeMillis): Boolean = {
        val maxWaitTime = lastModified + duration.toMillis
                
        now >= maxWaitTime 
      }
      
      override def updateLastAccessed: State[A] = copy(lastAccessed = System.currentTimeMillis)
    }
    
    def apply[A](valueAttempt: Try[A], now: Long = System.currentTimeMillis): WithValue[A] = {
      WithValue(now, now, valueAttempt)
    }
  }
}
