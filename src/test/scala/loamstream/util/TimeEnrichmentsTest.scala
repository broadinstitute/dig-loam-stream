package loamstream.util

import org.scalatest.FunSuite
import java.time.Instant

/**
 * @author clint
 * date: Aug 11, 2016
 */
final class TimeEnrichmentsTest extends FunSuite {
  test("Ordering on Instants") {
    import TimeEnrichments._
    
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