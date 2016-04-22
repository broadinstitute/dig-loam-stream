package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.signatures.Signatures.{SampleId, VariantId}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  case object LBoolean extends LType[Boolean]

  case object LDouble extends LType[Double]

  case object LFloat extends LType[Float]

  case object LLong extends LType[Long]

  case object LInt extends LType[Int]

  case object LShort extends LType[Short]

  case object LChar extends LType[Char]

  case object LByte extends LType[Byte]

  case object LString extends LType[String]

  case object LVariantId extends LType[VariantId]

  case object LSampleId extends LType[SampleId]

  case object LGenotype extends LType[Genotype]

  sealed trait LIterable[I <: Iterable[_]] extends LType[I] {
    def elementType: LTypeAny
  }

  case class LSet(elementType: LTypeAny) extends LIterable[Set[_]] {
  }

  case class LSeq(elementType: LTypeAny) extends LIterable[Seq[_]] {
  }

  object LTuple {
    def apply(tpe: LTypeAny, types: LTypeAny*): LTuple = LTuple(tpe +: types)
  }

  case class LTuple(types: Seq[LTypeAny]) extends LTypeAny {
    def arity: Int = types.size

    def apply(value: Any, values: Any*): LValue = LValue(value +: values, this)
  }

  case class LMap(keyType: LTypeAny, valueType: LTypeAny) extends LIterable[Map[_, _]] {
    override val elementType: LTuple = LTuple(keyType, valueType)
  }

}

sealed trait LTypeAny

sealed trait LType[T] extends LTypeAny {
  def apply(value: T): LValue = LValue(value, this)

  def of(any: Any): T = any.asInstanceOf[T]
}

