package loamstream

import htsjdk.variant.variantcontext.Genotype

import scala.reflect.runtime.universe.{Type, typeOf, TypeTag}

/**
  * @author clint
  *         date: Apr 26, 2016
  */
object Sigs {
  val variantAndSampleToGenotype: Type = typeOf[Map[(String, String), Genotype]]

  val sampleToSingletonCount: Type = typeOf[Map[String, Int]]

  val sampleIdAndIntToDouble: Type = typeOf[Map[(String, Int), Double]]

  val sampleIds: Type = typeOf[Set[String]]

  def sig[T: TypeTag]: Type = typeOf[T]
  
  def map[A: TypeTag, B: TypeTag]: Type = typeOf[Map[A,B]]
  
  def set[A: TypeTag]: Type = typeOf[Set[A]]
}