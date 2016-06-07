package loamstream

import htsjdk.variant.variantcontext.Genotype

import scala.reflect.runtime.universe.{Type, typeOf}

/**
  * @author clint
  *         date: Apr 26, 2016
  */
object Sigs {
  val variantAndSampleToGenotype: Type = typeOf[Map[(String, String), Genotype]]

  val sampleToSingletonCount: Type = typeOf[Map[String, Int]]

  val sampleIdAndIntToDouble: Type = typeOf[Map[(String, Int), Double]]

  val sampleIds: Type = typeOf[Set[String]]

}