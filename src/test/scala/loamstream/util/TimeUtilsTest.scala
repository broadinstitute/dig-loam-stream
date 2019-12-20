package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import scala.util.Failure
import java.time.Instant
import java.time.LocalDateTime

/**
 * @author clint
 * Mar 28, 2017
 */
final class TimeUtilsTest extends FunSuite {

  import TimeUtils.toEpochMilli
  
  test("toEpochMilli") {
    val t0 = LocalDateTime.now
    val t1 = t0.plusSeconds(42)
    
    val expectedOffsetInMs = 42 * 1000
    
    assert((toEpochMilli(t1) - toEpochMilli(t0)) === expectedOffsetInMs)
    assert((toEpochMilli(t0) - toEpochMilli(t1)) === -expectedOffsetInMs)
  }
  
  test("startAndEndTime") {
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { 42 }
    
    assert(attempt === Success(42))
    assert(toEpochMilli(start) <= toEpochMilli(end))
  }
  
  test("startAndEndTime - exception") {
    val e = new Exception
    
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { throw e }
    
    assert(attempt === Failure(e))
    assert(toEpochMilli(start) <= toEpochMilli(end))
  }
  
  test("Ordering on Instants") {
    import TimeUtils.Implicits._
    
    val now = Instant.now
    
    val before = now.minusMillis(1000)
    
    val after = now.plusMillis(10000)
    
    assert(now > before)
    assert(after > before)
    assert(before < now)
    assert(before < after)
    
    assert(now >= before)
    assert(after >= before)
    assert(before <= now)
    assert(before <= after)
    
    assert(now >= now)
    assert(now <= now)
    
    assert(before >= before)
    assert(before <= before)
    assert(after >= after)
    assert(after <= after)
  }
}
