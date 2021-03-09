package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.util.StringUtils
import loamstream.TestHelpers
import scala.util.Success

/**
 * @author clint
 */
final class TechTypeTest extends FunSuite {
  import TechType._
  
  test("name") {
    assert(Gwas.name === "GWAS")
    assert(ExChip.name === "ExChip")
    assert(ExSeq.name === "ExSeq")
    assert(Fm.name === "FM")
    assert(IChip.name === "IChip")
    assert(Wgs.name === "WGS")
    
    assert(Gwas.toString === "GWAS")
    assert(ExChip.toString === "ExChip")
    assert(ExSeq.toString === "ExSeq")
    assert(Fm.toString === "FM")
    assert(IChip.toString === "IChip")
    assert(Wgs.toString === "WGS")
  }
  
  test("values") {
    assert(values === Set(Gwas, ExChip, ExSeq, Fm, IChip, Wgs))
  }
  
  test("fromString/tryFromString - happy path") {
    def doTestShouldWork(expected: TechType): Unit = {
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
    
    doTestShouldWork(Gwas)
    doTestShouldWork(ExChip)
    doTestShouldWork(ExSeq)
    doTestShouldWork(Fm)
    doTestShouldWork(IChip)
    doTestShouldWork(Wgs)
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