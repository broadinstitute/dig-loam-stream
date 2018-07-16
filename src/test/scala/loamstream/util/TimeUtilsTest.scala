package loamstream.util

import org.scalatest.FunSuite
import scala.util.Success
import scala.util.Failure
import java.time.Instant

/**
 * @author clint
 * Mar 28, 2017
 */
final class TimeUtilsTest extends FunSuite {

  test("startAndEndTime") {
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { 42 }
    
    assert(attempt === Success(42))
    assert(start.toEpochMilli <= end.toEpochMilli)
  }
  
  test("startAndEndTime - exception") {
    val e = new Exception
    
    val (attempt, (start, end)) = TimeUtils.startAndEndTime { throw e }
    
    assert(attempt === Failure(e))
    assert(start.toEpochMilli <= end.toEpochMilli)
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
