package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 9, 2018
 */
final class SetsTest extends FunSuite {
  import Sets.fasterSetDiff
  
  test("fasterSetDiff") {
    val lhs = Set(1, 2, 3, 4)
    
    val rhs = Set(3, 4, 5, 6)
    
    assert(fasterSetDiff(lhs, rhs) === Set(1, 2))
    
    assert(fasterSetDiff(lhs, lhs) === Set.empty)
  }
  
  test("fasterSetDiff - some empty sets") {
    val s = Set("a", "b", "c") 
    
    assert(fasterSetDiff(Set.empty[String], Set.empty[String]) === Set.empty[String])
    
    assert(fasterSetDiff(s, Set.empty) === s)
    assert(fasterSetDiff(Set.empty, s) === Set.empty)
  }
}
