package loamstream.util

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: Jul 1, 2016
 * 
 * Useful utility methods for collections of Futures
 */
object Futures {
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
}