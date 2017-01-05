package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 23, 2016
 */
final class TerminableTest extends FunSuite {
  test("apply()") {
    var flag = 0
    
    val t = Terminable { flag += 1 }
    
    assert(flag === 0)
    
    t.stop()
    
    assert(flag === 1)
    
    t.stop()
    
    assert(flag === 2)
  }
}