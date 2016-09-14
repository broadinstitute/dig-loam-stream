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
  import Futures.waitFor
  import scala.concurrent.ExecutionContext.Implicits.global
  
  test("toMap / waitFor") {
    import Futures.toMap
    
    val empty: Iterable[(Int, Future[String])] = Nil
    
    assert(waitFor(toMap(empty)) == Map.empty)
    
    val tuples = Seq(42 -> Future("x"), 99 -> Future("y"))
    
    assert(waitFor(toMap(tuples)) == Map(42 -> "x", 99 -> "y"))
  }
  
  test("runBlocking / waitFor") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val f = Futures.runBlocking(42)
    
    //We can't easily tell that the code chunk was marked 'blocking', so just see that the right
    //result comes back.
    assert(waitFor(f) == 42)
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