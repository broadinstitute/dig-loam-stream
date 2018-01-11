package loamstream.util

import scala.concurrent.blocking
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * @author clint
 * date: Jul 1, 2016
 * 
 * Useful utility methods for collections of Futures
 */
object Futures {
  /**
   * Runs a block of code in a Future, marking the code chunk as blocking.
   * 
   * @param a the block of code to run
   */
  def runBlocking[A](a: => A)(implicit context: ExecutionContext): Future[A] = Future(blocking(a))
  
  /**
   * Folds a bunch of keys and future-values into a future map of keys to values
   * 
   * @param tuples a collection of 2-tuples containing a key of type A, and a future value of type Future[B].
   * @param context the ExecutionContext to run on
   * @return a future map of keys to values
   */
  def toMap[A,B](tuples: Traversable[(A, Future[B])])(implicit context: ExecutionContext): Future[Map[A,B]] = {
    val z: Future[Map[A,B]] = Future.successful(Map.empty)
    
    tuples.foldLeft(z) { (futureAcc, tuple) =>
      val (a, futureB) = tuple
      
      for {
        acc <- futureAcc
        b <- futureB
      } yield {
        acc + (a -> b)
      }
    }
  }
  
  object Implicits {
    final implicit class FutureOps[A](val fut: Future[A]) extends AnyVal {
      /**
       * Return a new Future that contains the result of fut, and ensures that the passed-in function f
       * completes before the returned future completes.  This ordering guarantee is more than what 
       * Future.foreach / Future.onComplete provides. 
       */
      def withSideEffect(f: A => Any)(implicit context: ExecutionContext): Future[A] = {
        //NB: Use map here instead of foreach to ensure that side-effects happen before the resulting
        //future is done.
        fut.map { a =>
          f(a)
          
          a
        }
      }
    }
  }
}
