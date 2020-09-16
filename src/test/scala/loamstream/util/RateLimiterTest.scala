package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import scala.util.Try


/**
 * @author clint
 * Sep 15, 2020
 */
final class RateLimiterTest extends FunSuite {
  test("Apply") {
    import scala.concurrent.duration._
    
    var timesInkoked = 0
    
    val delegate: () => Try[Int] = { () => 
      try { Success(42) } finally { timesInkoked += 1 }
    }
    
    val limiter = RateLimiter.withMaxAge(1.second) {
      delegate()
    }
    
    assert(timesInkoked === 0)
    
    assert(limiter() === Success(42))
    
    assert(timesInkoked === 1)
    
    assert(limiter().isFailure)
    
    assert(timesInkoked === 1)
    
    assert(limiter().isFailure)
    
    assert(timesInkoked === 1)
    
    assert(limiter().isFailure)
    
    assert(timesInkoked === 1)
    
    Thread.sleep(1.1.seconds.toMillis)
    
    assert(limiter() === Success(42))
    
    assert(timesInkoked === 2)
    
    assert(limiter().isFailure)
    
    assert(timesInkoked === 2)
    
    assert(limiter().isFailure)
    
    assert(timesInkoked === 2)
  }
}
