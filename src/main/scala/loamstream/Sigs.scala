package loamstream

import loamstream.model.piles.LSig
import loamstream.model.values.LType._
import loamstream.model.values.LType

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Sigs {
  //TODO: TEST
  val variantAndSampleToGenotype: LSig.Map = (LVariantId & LSampleId) to LGenotype

  //TODO: TEST
  val sampleToSingletonCount: LSig.Map = LSampleId to LSingletonCount

  //TODO: TEST
  val sampleIdAndIntToDouble: LSig.Map = (LSampleId & LInt) to LDouble
  
  //TODO: TEST
  def setOf[K](k: LType[K]): LSig.Set = LSig.Set.of(k)
}