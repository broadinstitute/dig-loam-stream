package loamstream.util

import scala.util.Try
import scala.concurrent.duration._
import scala.util.Success

/**
 * @author clint
 * Apr 24, 2019
 */
object Loops {
  private[util] val defaultDelayStart: Duration = 0.5.seconds
  private[util] val defaultDelayCap: Duration = 30.seconds
  
  private[util] def delaySequence(start: Duration, cap: Duration): Iterator[Duration] = {
    require(start gt 0.seconds)
    require(cap gt 0.seconds)
    
    Iterator.iterate(start)(_ * 2).map(_.min(cap))
  }
  
  def retryUntilSuccessWithBackoff[A](
      maxRuns: Int, 
      delayStart: Duration, 
      delayCap: Duration)(op: => Try[A]): Option[A] = {
    
    val delays = delaySequence(delayStart, delayCap)
    
    def delayIfFailure[A](attempt: Try[A]): Try[A] = {
      if(attempt.isFailure) {
        //TODO: Evaluate whether or not blocking is ok. For now, it's expedient and doesn't seem to cause problems.
        Thread.sleep(delays.next().toMillis)
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
