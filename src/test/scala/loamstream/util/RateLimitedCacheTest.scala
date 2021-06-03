package loamstream.util

import org.scalatest.FunSuite
import scala.concurrent.duration._
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Sep 8, 2020
 */
final class RateLimitedCacheTest extends FunSuite {
  test("Happy path") {
    @volatile var timesInvoked = 0
    
    def countInvocationAfter[P, A](body: () => A): P => A = { _ => 
      try { body() } finally { timesInvoked += 1 }
    }
    
    val cache = new RateLimitedCache[Unit, Int](countInvocationAfter(() => Success(42)), 1.second)
    
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
  
  test("Happy path - operation fails") {
    @volatile var timesInvoked = 0
    
    def countInvocationAfter[P, A](body: () => A): P => A = { _ => 
      try { body() } finally { timesInvoked += 1 }
    }
    
    val e = new Exception with scala.util.control.NoStackTrace
    
    val cache = new RateLimitedCache(countInvocationAfter(() => Failure(e)), 1.second)
    
    assert(timesInvoked === 0)
    
    assert(cache() === Failure(e))
    
    assert(timesInvoked === 1)
    
    assert(cache() === Failure(e))
    assert(cache() === Failure(e))
    
    assert(timesInvoked === 1)
    
    Thread.sleep(1100)
    
    assert(cache() === Failure(e))
    
    assert(timesInvoked === 2)
  }
  
  test("State.lastModified") {
    assert(RateLimitedCache.State.Initial.lastModified === Long.MinValue)
    
    assert(RateLimitedCache.State.WithValue(123, 456, Success(42)).lastModified === 123)
  }
  
  test("State.lastAccessed") {
    assert(RateLimitedCache.State.Initial.lastAccessed === Long.MinValue)
    
    assert(RateLimitedCache.State.WithValue(123, 456, Success(42)).lastAccessed === 456)
  }
  
  test("State.updateLastAccessed") {
    intercept[Exception] {
      RateLimitedCache.State.Initial.updateLastAccessed
    }
    
    val first = RateLimitedCache.State.WithValue(123, 456, Success(42))
    
    val updated = first.updateLastAccessed
    
    assert(first.lastModified === updated.lastModified)
    assert(first.lastValue === updated.lastValue)
    assert(first.lastAccessed < updated.lastAccessed)
  }
  
  test("State.apply") {
    val actual = RateLimitedCache.State(Success(42), now = 123)
    
    assert(actual === RateLimitedCache.State.WithValue(123, 123, Success(42)))
  }
  
  test("WithValue.hasBeenLongerThan") {
    val s: RateLimitedCache.State.WithValue[Int] = RateLimitedCache.State(Success(42), now = 123)
    
    import scala.concurrent.duration._
    
    assert(s.hasBeenLongerThan(10.millis, now = 123) === false)

    assert(s.hasBeenLongerThan(0.seconds, now = 123) === true)
    assert(s.hasBeenLongerThan(1.millis, now = 124) === true)
    assert(s.hasBeenLongerThan(10.millis, now = 133) === true)
    assert(s.hasBeenLongerThan(10.millis, now = 143) === true)
    assert(s.hasBeenLongerThan(10.millis, now = 1000) === true)
  }
}
