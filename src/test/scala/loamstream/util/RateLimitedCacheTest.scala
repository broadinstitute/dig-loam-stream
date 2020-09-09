package loamstream.util

import org.scalatest.FunSuite
import scala.concurrent.duration._
import scala.util.Success

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedCacheTest extends FunSuite {
  test("Happy path") {
    @volatile var timesInvoked = 0
    
    def countInvocationAfter[A](body: () => A): () => A = { () => 
      try { body() } finally { timesInvoked += 1 }
    }
    
    val cache = new RateLimitedCache(countInvocationAfter(() => Success(42)), 1.second)
    
    assert(timesInvoked === 0)
    
    assert(cache() === Success(42))
    
    assert(timesInvoked === 1)
    
    assert(cache() === Success(42))
    assert(cache() === Success(42))
    
    assert(timesInvoked === 1)
    
    Thread.sleep(1100)
    
    assert(cache() === Success(42))
    
    assert(timesInvoked === 2)
  }
}
