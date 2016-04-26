package loamstream

import org.scalatest.FunSuite
import loamstream.model.values.LType
import loamstream.model.piles.LSig

/**
 * @author clint
 * date: Apr 26, 2016
 */
final class SigsTest extends FunSuite {
  import LType._
  import LType.LTuple._
  
  test("setOf()") {
    assert(Sigs.setOf(LInt) === LSig.Set(LTuple1(LInt)))
    
    assert(Sigs.setOf(LVariantId) === LSig.Set(LTuple1(LVariantId)))
  }
  
  test("Built-in sigs") {  
    import Sigs._
    
    assert(variantAndSampleToGenotype === ((LVariantId & LSampleId) to LGenotype))

    assert(sampleToSingletonCount === (LSampleId to LSingletonCount))

    assert(sampleIdAndIntToDouble === ((LSampleId & LInt) to LDouble))
  }
}