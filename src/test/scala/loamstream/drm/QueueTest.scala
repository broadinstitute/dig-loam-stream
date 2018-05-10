package loamstream.drm

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 15, 2017
 */
final class QueueTest extends FunSuite {
  
  test("name/toString") {
    val asdf = Queue("asdf")
    
    assert(asdf.name === "asdf")
    assert(asdf.toString === "asdf")
  }
  
  test("guards") {
    Queue("asdf")
    
    intercept[Exception] {
      Queue("")
    }
  }
}
