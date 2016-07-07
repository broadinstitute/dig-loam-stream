package loamstream.util

import org.scalatest.FunSuite
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * @author clint
 * date: Jul 1, 2016
 */
final class FuturesTest extends FunSuite {
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)
  
  test("toMap") {
    import Futures.toMap
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val empty: Iterable[(Int, Future[String])] = Nil
    
    assert(waitFor(toMap(empty)) == Map.empty)
    
    val tuples = Seq(42 -> Future("x"), 99 -> Future("y"))
    
    assert(waitFor(toMap(tuples)) == Map(42 -> "x", 99 -> "y"))
  }
}