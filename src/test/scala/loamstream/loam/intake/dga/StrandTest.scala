package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import loamstream.util.StringUtils
import loamstream.TestHelpers
import scala.util.Success

/**
 * @author clint
 * 22 Apr, 2021
 */
final class StrandTest extends FunSuite {
  import Strand._
  
  test("name") {
    assert(Plus.name === "+")
    assert(+.name === "+")
    assert(Minus.name === "-")
    assert(-.name === "-")
  }
  
  test("values") {
    assert(values === Set(Plus, Minus))
  }
  
  test("fromString") {
    def doTest(s: Strand): Unit = {
      assert(fromString(s.name) === Some(s))
      assert(fromString(s.name.toUpperCase) === Some(s))
      assert(fromString(TestHelpers.to1337Speak(s.name)) === Some(s))
    }
    
    def doTestDoesntWork(s: String): Unit = assert(fromString(s) === None)
    
    doTest(Plus)
    doTest(Minus)
    
    doTestDoesntWork("plus")
    doTestDoesntWork("minus")
    doTestDoesntWork("   ")
    doTestDoesntWork("als;kdfjsdlf")
  }
  
  test("tryFromString") {
    def doTest(s: Strand): Unit = {
      assert(tryFromString(s.name) === Success(s))
      assert(tryFromString(s.name.toUpperCase) === Success(s))
      assert(tryFromString(TestHelpers.to1337Speak(s.name)) === Success(s))
    }
    
    def doTestDoesntWork(s: String): Unit = assert(tryFromString(s).isFailure)
    
    doTest(Plus)
    doTest(Minus)
    
    doTestDoesntWork("plus")
    doTestDoesntWork("minus")
    doTestDoesntWork("   ")
    doTestDoesntWork("als;kdfjsdlf")
  }
}