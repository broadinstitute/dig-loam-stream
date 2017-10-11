package loamstream.uger

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 15, 2017
 */
final class QueueTest extends FunSuite {
  import Queue._
  
  test("fromString") {
    assert(fromString("") === None)
    assert(fromString("   ") === None)
    assert(fromString("asdasdasdasd") === None)
    
    assert(fromString("short") === None)
    assert(fromString("Short") === None)
    assert(fromString("SHORT") === None)
    assert(fromString("ShOrT") === None)
    
    assert(fromString("long") === None)
    assert(fromString("Long") === None)
    assert(fromString("LONG") === None)
    assert(fromString("LoNg") === None)
    
    assert(fromString("broad") === Some(Broad))
    assert(fromString("Broad") === Some(Broad))
    assert(fromString("BROAD") === Some(Broad))
    assert(fromString("bRoAd") === Some(Broad))
  }
  
  test("name") {
    assert(Broad.name === "broad")
  }
  
  test("isBroad") {
    assert(Broad.isBroad === true)
  }
  
  test("Default") {
    assert(Queue.Default === Broad)
  }
}
