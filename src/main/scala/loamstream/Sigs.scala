package loamstream

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig

/**
  * @author clint
  *         date: Apr 26, 2016
  */
object Sigs {
  val variantAndSampleToGenotype: LSig = LSig.create[Map[(String, String), Genotype]]

  val sampleToSingletonCount: LSig = LSig.create[Map[String, Int]]

  val sampleIdAndIntToDouble: LSig = LSig.create[Map[(String, Int), Double]]

  val sampleIds: LSig = LSig.create[String]

}