package loamstream

import loamstream.model.LSig
import loamstream.model.values.LType._
import loamstream.model.values.LType

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Sigs {
  val variantAndSampleToGenotype: LSig.Map = (LVariantId & LSampleId) to LGenotype

  val sampleToSingletonCount: LSig.Map = LSampleId to LSingletonCount

  val sampleIdAndIntToDouble: LSig.Map = (LSampleId & LInt) to LDouble
  
  def setOf[K](k: LType[K]): LSig.Set = LSig.Set.of(k)
}