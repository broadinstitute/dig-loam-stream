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
  
  import scala.concurrent.ExecutionContext.Implicits.global
  
  test("toMap") {
    import Futures.toMap
    
    val empty: Iterable[(Int, Future[String])] = Nil
    
    assert(waitFor(toMap(empty)) == Map.empty)
    
    val tuples = Seq(42 -> Future("x"), 99 -> Future("y"))
    
    assert(waitFor(toMap(tuples)) == Map(42 -> "x", 99 -> "y"))
  }
  
  test("withSideEffect") {
    //Single side effect
    {
      var x = 0

      import Futures.Implicits._
      
      assert(x == 0)
      
      val f = Future("asdf").withSideEffect(s => x += s.size)
      
      waitFor(f)
      
      assert(x == 4)
    }
    
    //multiple side effects
    {
      var x = 0
      
      val se = (s: String) => x += s.size
      
      assert(x == 0)
      
      import Futures.Implicits._      
      
      val f = Future("asdf").withSideEffect(se).withSideEffect(se).withSideEffect(se)
      
      waitFor(f)
      
      assert(x == 12)
    }
  }
}