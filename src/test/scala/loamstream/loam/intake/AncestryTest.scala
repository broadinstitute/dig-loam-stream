package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.util.StringUtils
import loamstream.TestHelpers
import scala.util.Success

/**
 * @author clint
 */
final class AncestryTest extends FunSuite {
  import Ancestry._
  test("name") {
    assert(AA.name === "AA")
    assert(AF.name === "AF")
    assert(EA.name === "EA")
    assert(EU.name === "EU")
    assert(HS.name === "HS")
    assert(Mixed.name === "Mixed")
    assert(SA.name === "SA")
    
    assert(AA.toString === "AA")
    assert(AF.toString === "AF")
    assert(EA.toString === "EA")
    assert(EU.toString === "EU")
    assert(HS.toString === "HS")
    assert(Mixed.toString === "Mixed")
    assert(SA.toString === "SA")
  }
  
  test("values") {
    assert(values === Set(AA, AF, EA, EU, HS, Mixed, SA))
  }
  
  test("fromString/tryFromString - happy path") {
    def doTestShouldWork(expected: Ancestry): Unit = {
      def unmarshal(stringRep: String): Unit = {
        assert(fromString(stringRep) === Some(expected))
        assert(tryFromString(stringRep) === Success(expected))
      }
      
      import expected.name
      
      unmarshal(name)
      unmarshal(name.toUpperCase)
      unmarshal(name.toLowerCase)
      unmarshal(TestHelpers.to1337Speak(name))
    }
    
    doTestShouldWork(AA)
    doTestShouldWork(AF)
    doTestShouldWork(EA)
    doTestShouldWork(EU)
    doTestShouldWork(HS)
    doTestShouldWork(Mixed)
    doTestShouldWork(SA)
  }
  
  test("fromString/tryFromString - bad input") {
    def doTestShouldNOTWork(bad: String): Unit = {
      assert(fromString(bad) === None)
      assert(tryFromString(bad).isFailure)
    }
    
    doTestShouldNOTWork("")
    doTestShouldNOTWork("lalala")
    doTestShouldNOTWork("   ")
  }
}