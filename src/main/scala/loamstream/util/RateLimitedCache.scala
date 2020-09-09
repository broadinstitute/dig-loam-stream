package loamstream.util

import java.time.LocalDateTime
import RateLimitedCache.State
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedCache[B](delegate: () => Try[B], maxAge: Duration) extends (() => Try[B]) {
  private val stateBox: ValueBox[State[B]] = ValueBox(State.Initial)
  
  def lastAccessed: Long = stateBox.get {
    case State.Initial => Long.MinValue
    case s @ State.WithValue(_, _, _) => s.lastAccessed 
  }
  
  def lastModified: Long = stateBox.get {
    case State.Initial => Long.MinValue
    case s @ State.WithValue(_, _, _) => s.lastModified 
  }
  
  override def apply(): Try[B] = stateBox.getAndUpdate { state =>
    val shouldRun: Boolean = state match {
      case State.Initial => true
      case s @ State.WithValue(_, _, _) => s.hasBeenLongerThan(maxAge)
    }
    
    if(shouldRun) {
      val valueAttempt: Try[B] = delegate()
      
      (State(valueAttempt), valueAttempt)
    } else {
      (state, state.lastValue)
    }
  }
}

object RateLimitedCache {
  sealed trait State[+A] {
    def lastModified: Long
    
    def lastAccessed: Long
    
    def lastValue: Try[A]
  }
  
  object State {
    case object Initial extends State[Nothing] {
      override def lastModified: Long = sys.error("No last modified time")
    
      override def lastAccessed: Nothing = sys.error("No last accessed time")
      
      override def lastValue: Nothing = sys.error("No last value")
    }

    final case class WithValue[A](lastModified: Long, lastAccessed: Long, lastValue: Try[A]) extends State[A] {
      def hasBeenLongerThan(duration: Duration): Boolean = {
        val now = System.currentTimeMillis
        
        val maxWaitTime = lastModified + duration.toMillis
                
        now >= maxWaitTime 
      }
    }
    
    def apply[A](valueAttempt: Try[A]): WithValue[A] = {
      val now = System.currentTimeMillis
      
      WithValue(now, now, valueAttempt)
    }
  }
}
