package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Apr 26, 2016
 */
final class FunctionsTest extends FunSuite {
  test("memoize() should work for functions with no args") {
    import Functions.memoize
    
    var invocations = 0
    
    val f: () => Int = { () =>
      try 42 + invocations finally invocations += 1
    }
    
    assert(invocations === 0)
    
    val memoized = memoize(f)
    
    assert(invocations === 0)
    
    assert(memoized() === 42)
    
    assert(invocations === 1)
    
    assert(memoized() === 42)
    
    assert(invocations === 1)
    
    assert(memoized() === 42)
    
    assert(invocations === 1)
  }
  
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
  
  test("memoize() with shouldCache filter should work") {
    import Functions.memoize
    
    var invocations = 0
    
    val f: Int => String = { i =>
      invocations += 1
      
      i.toString
    }
    
    def isEven(i: Int): Boolean = i % 2 == 0
    
    //Only cache "even" results
    val shouldCache: String => Boolean = s => isEven(s.toInt)  
    
    val memoized = memoize(f, shouldCache)
    
    assert(invocations === 0)
    
    assert(memoized(1) === "1")
    
    assert(invocations === 1)
    
    assert(memoized(1) === "1")
    assert(memoized(1) === "1")
    assert(memoized(1) === "1")
    
    assert(invocations === 4)
    
    assert(memoized(3) === "3")
    
    assert(invocations === 5)
    
    assert(memoized(2) === "2")
    
    assert(invocations === 6)
    
    assert(memoized(2) === "2")
    assert(memoized(2) === "2")
    assert(memoized(2) === "2")
    
    assert(invocations === 6)
    
    assert(memoized(3) === "3")
    
    assert(invocations === 7)
  }
}
