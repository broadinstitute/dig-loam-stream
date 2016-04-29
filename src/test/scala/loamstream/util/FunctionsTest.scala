package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Apr 26, 2016
 */
final class FunctionsTest extends FunSuite {
  test("memoize() should work") {
    import Functions.memoize
    
    var invocations = 0
    
    val f: Int => String = { i =>
      invocations += 1
      
      i.toString
    }
    
    val memoized = memoize(f)
    
    assert(invocations === 0)
    
    assert(memoized(1) === "1")
    
    assert(invocations === 1)
    
    assert(memoized(1) === "1")
    assert(memoized(1) === "1")
    assert(memoized(1) === "1")
    
    assert(invocations === 1)
    
    assert(memoized(3) === "3")
    
    assert(invocations === 2)
    
    assert(memoized(2) === "2")
    
    assert(invocations === 3)
    
    assert(memoized(3) === "3")
    
    assert(invocations === 3)
    
    assert(memoized(2) === "2")
    
    assert(invocations === 3)
  }
}