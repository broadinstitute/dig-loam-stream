package loamstream.util

import scala.util.Try
import scala.concurrent.duration._
import scala.util.Success

/**
 * @author clint
 * Apr 24, 2019
 */
object Loops {
  
  private[util] object Backoff {
    def delaySequence(start: Duration, cap: Duration): Iterator[Duration] = {
      require(start gt 0.seconds)
      require(cap gt 0.seconds)
      
      Iterator.iterate(start)(_ * 2).map(_.min(cap))
    }
  }
  
  /**
   * Perform an operation that might fail, and returns a Try, up to maxRuns times, returning the result, if any,
   * of the last success, wrapped in an Option, or None if no operation was successful.  In between failures, wait
   * a certain amount of time, starting at the duration indicated by delayStart after the first failure and doubling
   * after each subsequent failure, up to a max of delayCap.  @see Backoff.delaySequence .
   */
  def retryUntilSuccessWithBackoff[A](
      maxRuns: Int, 
      delayStart: Duration, 
      delayCap: Duration)(op: => Try[A]): Option[A] = {
    
    val delays = Backoff.delaySequence(delayStart, delayCap)
    
    def delayIfFailure[A](attempt: Try[A]): Try[A] = {
      if(attempt.isFailure) {
        val howMuch = delays.next().toMillis
        
        if(howMuch > 0) {
          //TODO: Evaluate whether or not blocking is ok. For now, it's expedient and doesn't seem to cause problems.
          Thread.sleep(delays.next().toMillis)
        }
      }
      
      attempt
    }
    
    val attempts = Iterator.continually(op).take(maxRuns).map(delayIfFailure).dropWhile(_.isFailure)
    
    attempts.toStream.headOption match {
      case Some(Success(a)) => Some(a)
      case _ => None
    }
  }
}
