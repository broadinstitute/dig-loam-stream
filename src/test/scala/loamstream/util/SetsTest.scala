package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 9, 2018
 */
final class SetsTest extends FunSuite {
  import Sets.hashSetDiff
  
  test("hashSetDiff") {
    val lhs = Set(1, 2, 3, 4)
    
    val rhs = Set(3, 4, 5, 6)
    
    assert(hashSetDiff(lhs, rhs) === Set(1, 2))
    
    assert(hashSetDiff(lhs, lhs) === Set.empty)
  }
  
  test("hashSetDiff - some empty sets") {
    val s = Set("a", "b", "c") 
    
    assert(hashSetDiff(Set.empty[String], Set.empty[String]) === Set.empty[String])
    
    assert(hashSetDiff(s, Set.empty) === s)
    assert(hashSetDiff(Set.empty, s) === Set.empty)
  }
}
