package loamstream.util

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import monix.execution.Scheduler
import monix.reactive.Observable
import monix.eval.Task

/**
 * @author clint
 * Apr 24, 2019
 */
object Loops {
  
  //TODO: Investigate replacing with Task.onErrorRetryLoop
  
  private[util] object Backoff {
    def delaySequence(start: FiniteDuration, cap: FiniteDuration): Iterator[FiniteDuration] = {
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
      delayStart: FiniteDuration, 
      delayCap: FiniteDuration)(op: => Try[A]): Option[A] = {
    
    val delays = Backoff.delaySequence(delayStart, delayCap)
    
    def delayIfFailure[A](attempt: Try[A]): Try[A] = {
      if(attempt.isFailure) {
        val howMuch = delays.next().toMillis
        
        if(howMuch > 0) {
          //TODO: Evaluate whether or not blocking is ok. For now, it's expedient and doesn't seem to cause problems.
          Thread.sleep(howMuch)
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
  
  def retryUntilSuccessWithBackoffAsync[A](
      maxRuns: Int, 
      delayStart: FiniteDuration, 
      delayCap: FiniteDuration,
      scheduler: Scheduler)(op: => Observable[Try[A]]): Observable[Option[A]] = {
    
    val delays = Backoff.delaySequence(delayStart, delayCap)
    
    def delayAndThen[X](f: => X): Observable[X] = Observable.evalDelayed(delays.next(), f)
    
    def next(tuple: (Int, Observable[Try[A]])): Observable[(Int, Try[A])] = {
      val (i, attemptObs) = tuple
      
      attemptObs.flatMap {
        case Success(_) => attemptObs.map(attempt => (i, attempt))
        case Failure(_) if i >= maxRuns => Observable.empty
        case _ => delayAndThen((i + 1) -> op).flatMap(next)
      }
    }
    
    if(maxRuns == 0) { 
      Observable(None)
    } else {
      import Observables.Implicits._
      
      next(1 -> op).collect { case (_, attempt) => attempt.toOption }.firstOption.map(_.flatten)
    }
  }
}
