package loamstream

import org.scalatest.FunSuite
import loamstream.model.values.LType
import loamstream.model.LSig

/**
 * @author clint
 * date: Apr 26, 2016
 */
final class SigsTest extends FunSuite {
  import LType._
  import LType.LTuple._
  
  test("setOf()") {
    assert(Sigs.setOf(LInt) === LSig.Set(LTuple1(LInt)))
    
    assert(Sigs.setOf(LString) === LSig.Set(LTuple1(LString)))
  }
  
  test("Built-in sigs") {  
    import Sigs._
    
    assert(variantAndSampleToGenotype === ((LString & LString) to LGenotype))

    assert(sampleToSingletonCount === (LString to LSingletonCount))

    assert(sampleIdAndIntToDouble === ((LString & LInt) to LDouble))
  }
}