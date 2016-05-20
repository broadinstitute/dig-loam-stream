package loamstream.model.values

import LType._
import loamstream.model.Types.SingletonCount
import htsjdk.variant.variantcontext.Genotype

/**
 * @author clint
 * date: Apr 28, 2016
 */
sealed trait HasLType[A] {
  def lType: LType
}

object HasLType {
  private def toHasLType[A](lt: LType): HasLType[A] = new HasLType[A] { override def lType = lt }

  implicit val BooleanHasLType: HasLType[Boolean] = toHasLType(LBoolean)
  implicit val DoubleHasLType: HasLType[Double] = toHasLType(LDouble)
  implicit val FloatHasLType: HasLType[Float] = toHasLType(LFloat)
  implicit val LongHasLType: HasLType[Long] = toHasLType(LLong)
  implicit val IntHasLType: HasLType[Int] = toHasLType(LInt)
  implicit val ShortHasLType: HasLType[Short] = toHasLType(LShort)
  implicit val StringHasLType: HasLType[String] = toHasLType(LString)
  //implicit val VariantIdHasLType: HasLType[VariantId] = toHasLType(LVariantId)
  //implicit val SampleIdHasLType: HasLType[SampleId] = toHasLType(LSampleId)
  implicit val GenotypeHasLType: HasLType[Genotype] = toHasLType(LGenotype)
  implicit val SingletonCountHasLType: HasLType[SingletonCount] = toHasLType(LSingletonCount)
  //implicit val ClusterIdHasLType: HasLType[ClusterId] = toHasLType(LClusterId)
  implicit def setHasLType[E: HasLType]: HasLType[Set[E]] = toHasLType(LSet(implicitly[HasLType[E]].lType))
  implicit def seqHasLType[E: HasLType]: HasLType[Seq[E]] = toHasLType(LSeq(implicitly[HasLType[E]].lType))
}