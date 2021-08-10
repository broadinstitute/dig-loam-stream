package loamstream.util

import org.scalatest.FunSuite
import java.io.Closeable

/**
 * @author clint
 * Nov 23, 2016
 */
final class TerminableTest extends FunSuite {
  
  test("apply()") {
    doTerminableTest(Terminable(_))
  }
  
  test("from Closeable") {
    doTerminableTest(block => Terminable.from(closeable(block)))
  }
  
  test("asCloseable") {
    doTerminableTest(block => Terminable(block).asCloseable)
  }
  
  test("asTerminable") {
    import Terminable.CloseableOps
    
    doTerminableTest(block => closeable(block).asTerminable)
  }
  
  test("StopsComponents - terminables") {
    var flag0 = 0
    var flag1 = 0
    
    val t0 = Terminable { flag0 += 1 }
    val t1 = Terminable { flag1 += 1 }
    
    val sc: Terminable = Terminable.StopsComponents(t0, t1)
    
    assert(flag0 === 0)
    assert(flag1 === 0)
    
    sc.stop()
    
    assert(flag0 === 1)
    assert(flag1 === 1)
    
    sc.stop()
    
    assert(flag0 === 2)
    assert(flag1 === 2)
  }
  
  test("StopsComponents - closeables") {
    var flag0 = 0
    var flag1 = 0
    
    val c0 = closeable { flag0 += 1 }
    val c1 = closeable { flag1 += 1 }
    
    val sc: Terminable = Terminable.StopsComponents(c0, c1)
    
    assert(flag0 === 0)
    assert(flag1 === 0)
    
    sc.stop()
    
    assert(flag0 === 1)
    assert(flag1 === 1)
    
    sc.stop()
    
    assert(flag0 === 2)
    assert(flag1 === 2)
  }
  
  test("stopAfter") {
    var flag0 = 0
    var flag1 = 0
    
    val t0 = Terminable { flag0 += 1 }
    
    assert(flag0 === 0)
    assert(flag1 === 0)
    
    t0.stopAfter {
      flag1 += 1
      
      assert(flag0 === 0)
    }
    
    assert(flag0 == 1)
    assert(flag1 == 1)
  }
  
  private def doTerminableTest[T : CanBeClosed](make: (=> Any) => T): Unit = {
    import CanBeClosed.Syntax
    
    var flag = 0
    
    val t = make { flag += 1 }
    
    assert(flag === 0)
    
    t.close()
    
    assert(flag === 1)
    
    t.close()
    
    assert(flag === 2)
  }
  
  private def closeable(body: => Any): Closeable = new Closeable {
    override def close(): Unit = body
  }
}
