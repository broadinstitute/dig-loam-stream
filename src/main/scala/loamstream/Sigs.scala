package loamstream

import loamstream.model.LSig
import loamstream.model.values.LType._
import loamstream.model.values.LType

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Sigs {
  val variantAndSampleToGenotype: LSig.Map = (LString & LString) to LGenotype

  val sampleToSingletonCount: LSig.Map = LString to LInt

  val sampleIdAndIntToDouble: LSig.Map = (LString & LInt) to LDouble
  
  val sampleIds: LSig.Set = LSig.Set.of(LString)
  
  def setOf(k: LType): LSig.Set = LSig.Set.of(k)
}