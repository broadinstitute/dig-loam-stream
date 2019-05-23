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
  
  test("StopsComponents") {
    var flag0 = 0
    var flag1 = 0
    
    val t0 = Terminable { flag0 += 1 }
    val t1 = Terminable { flag1 += 1 }
    
    val sc: Terminable = new Terminable.StopsComponents with Loggable {
      override val terminableComponents = Seq(t0, t1)
    }
    
    assert(flag0 === 0)
    assert(flag1 === 0)
    
    sc.stop()
    
    assert(flag0 === 1)
    assert(flag1 === 1)
    
    sc.stop()
    
    assert(flag0 === 2)
    assert(flag1 === 2)
  }
}
