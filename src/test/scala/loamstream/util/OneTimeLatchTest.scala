package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 31, 2017
 */
final class OneTimeLatchTest extends FunSuite {
  test("doOnce") {
    var count = 0
    
    def inc(): Unit = count += 1
    
    val latch = new OneTimeLatch
    
    assert(count === 0)
    
    latch.doOnce(inc())
    
    assert(count === 1)
    
    latch.doOnce(inc())
    
    assert(count === 1)
    
    latch.doOnce(inc())
    
    assert(count === 1)
    
    latch.doOnce(inc())
    
    assert(count === 1)
  }
}
