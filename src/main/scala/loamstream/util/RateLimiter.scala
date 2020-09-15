package loamstream.util

import scala.util.Try
import scala.concurrent.duration.Duration
import java.time.LocalDateTime
import scala.util.Failure


/**
 * @author clint
 * Sep 15, 2020
 */
final class RateLimiter[A](delegate: () => Try[A], maxAge: Duration) extends (() => Try[A]) {
  private val lastInvocationBox: ValueBox[Option[Long]] = ValueBox(None)
  
  private def lastRun: Long = lastInvocationBox.value.getOrElse(Long.MinValue)
  
  private[util] def timeToRun(lastRan: Long, now: Long = System.currentTimeMillis): Boolean = {
    (lastRan + maxAge.toMillis) <= now
  }
  
  override def apply(): Try[A] = {
    val now = System.currentTimeMillis
    
    def newLastRunTimeAndResult: (Option[Long], Try[A]) = (Some(now), delegate())
    
    lastInvocationBox.getAndUpdate {
      case None => newLastRunTimeAndResult
      case Some(lastRan) if timeToRun(lastRan, now) => newLastRunTimeAndResult
      case lastRanOpt => {
        val failure: Try[A] = Failure(RateLimiter.NotTimeToRunException(now, lastRanOpt))
        
        (lastRanOpt, failure) 
      }
    }
  }
}

object RateLimiter {
  def withMaxAge[A](maxAge: Duration)(body: => Try[A]): RateLimiter[A] = new RateLimiter(() => body, maxAge)
  
  private def msg(now: Long, lastRan: Option[Long]): String = {
    s"Not yet time to run; now: ${now} last ran: ${lastRan.map(_.toString).getOrElse("Never")}"
  }
  
  final case class NotTimeToRunException(now: Long, lastRan: Option[Long]) extends Exception(msg(now, lastRan))
}
