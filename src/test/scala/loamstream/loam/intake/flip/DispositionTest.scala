package loamstream.loam.intake.flip

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 23, 2020
 */
final class DispositionTest extends FunSuite {
  test("flags") {
    import Disposition._
    
    assert(NotFlippedComplementStrand.isFlipped === false)
    assert(NotFlippedComplementStrand.isSameStrand === false)
    assert(NotFlippedComplementStrand.notFlipped === true)
    assert(NotFlippedComplementStrand.isComplementStrand === true)
    
    assert(FlippedComplementStrand.isFlipped === true)
    assert(FlippedComplementStrand.isSameStrand === false)
    assert(FlippedComplementStrand.notFlipped === false)
    assert(FlippedComplementStrand.isComplementStrand === true)
    
    assert(NotFlippedSameStrand.isFlipped === false)
    assert(NotFlippedSameStrand.isSameStrand === true)
    assert(NotFlippedSameStrand.notFlipped === true)
    assert(NotFlippedSameStrand.isComplementStrand === false)
    
    assert(FlippedSameStrand.isFlipped === true)
    assert(FlippedSameStrand.isSameStrand === true)
    assert(FlippedSameStrand.notFlipped === false)
    assert(FlippedSameStrand.isComplementStrand === false)
  }
}
