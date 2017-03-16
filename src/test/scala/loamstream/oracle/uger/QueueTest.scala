package loamstream.oracle.uger

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 15, 2017
 */
final class QueueTest extends FunSuite {
  import Queue._
  
  test("fromString") {
    assert(fromString("").isEmpty)
    assert(fromString("asdasdasdasd").isEmpty)
    
    assert(fromString("short") === Some(Short))
    assert(fromString("Short") === Some(Short))
    assert(fromString("SHORT") === Some(Short))
    assert(fromString("ShOrT") === Some(Short))
    
    assert(fromString("long") === Some(Long))
    assert(fromString("Long") === Some(Long))
    assert(fromString("LONG") === Some(Long))
    assert(fromString("LoNg") === Some(Long))
  }
  
  test("name") {
    assert(Short.name === "short")
    assert(Long.name === "long")
  }
  
  test("shorter/longer") {
    assert(Short.shorter === Short)
    assert(Short.longer === Long)
    
    assert(Long.shorter === Short)
    assert(Long.longer === Long)
  }
  
  test("isShort/isLong") {
    assert(Short.isShort === true)
    assert(Short.isLong === false)
    
    assert(Long.isShort === false)
    assert(Long.isLong === true)
  }
}
