package loamstream

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig

import scala.reflect.runtime.universe.typeOf

/**
  * @author clint
  *         date: Apr 26, 2016
  */
object Sigs {
  val variantAndSampleToGenotype: LSig = LSig(typeOf[Map[(String, String), Genotype]])

  val sampleToSingletonCount: LSig = LSig(typeOf[Map[String, Int]])

  val sampleIdAndIntToDouble: LSig = LSig(typeOf[Map[(String, Int), Double]])

  val sampleIds: LSig = LSig(typeOf[String])

}