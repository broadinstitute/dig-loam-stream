package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Aug 8, 2016
 */
final class TakesEndingActionIteratorTest extends FunSuite {
  test("onClose invoked when expected") {
    var flag = 0
    
    val is = Seq(1,2,3).iterator
    
    val iterator = TakesEndingActionIterator(is)(flag += 1)
    
    assert(is.hasNext)
    assert(iterator.hasNext)
    
    assert(iterator.next() == 1)
    
    assert(flag == 0)
    assert(is.hasNext)
    assert(iterator.hasNext)
    
    assert(iterator.next() == 2)
    
    assert(flag == 0)
    assert(is.hasNext)
    assert(iterator.hasNext)
    
    assert(iterator.next() == 3)
    
    assert(flag == 1)
    assert(!is.hasNext)
    assert(!iterator.hasNext)
    
    intercept[Exception] {
      iterator.next()
    }
    
    assert(flag == 1)
  }
}