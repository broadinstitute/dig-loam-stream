package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.signatures.Signatures.{SampleId, VariantId}
import loamstream.model.values.LType.Encodeable
import loamstream.util.shot.{Hit, Miss, Shot}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  sealed trait Encodeable extends LTypeAny {
    override def asEncodeable: Hit[Encodeable] = Hit(this)
  }

  case object LBoolean extends LType[Boolean] with Encodeable

  case object LDouble extends LType[Double] with Encodeable

  case object LFloat extends LType[Float] with Encodeable

  case object LLong extends LType[Long] with Encodeable

  case object LInt extends LType[Int] with Encodeable

  case object LShort extends LType[Short] with Encodeable

  case object LChar extends LType[Char] with Encodeable

  case object LByte extends LType[Byte] with Encodeable

  case object LString extends LType[String] with Encodeable

  case object LVariantId extends LType[VariantId] with Encodeable

  case object LSampleId extends LType[SampleId] with Encodeable

  case object LGenotype extends LType[Genotype]

  sealed trait LIterable[I <: Iterable[_]] extends LType[I] {
    def elementType: LTypeAny
  }

  case class LSet(elementType: LTypeAny) extends LIterable[Set[_]] with Encodeable {
  }

  case class LSeq(elementType: LTypeAny) extends LIterable[Seq[_]] with Encodeable {
  }

  object LTuple {
    def apply(tpe: Encodeable, types: Encodeable*): LTuple = LTuple(tpe +: types)
  }

  case class LTuple(types: Seq[Encodeable]) extends Encodeable {
    def arity: Int = types.size

    def apply(value: Any, values: Any*): LValue = LValue(value +: values, this)
  }

  case class LMap(keyType: Encodeable, valueType: Encodeable) extends LIterable[Map[_, _]] {
    override val elementType: LTuple = LTuple(keyType, valueType)
  }

}

sealed trait LTypeAny {
  def asEncodeable: Shot[Encodeable] = Miss(s"Type '$this' does not represent an encodeable value.")
}

sealed trait LTypeTyped[T] extends LTypeAny {
  def of(any: Any): T = any.asInstanceOf[T]
}

sealed trait LType[T] extends LTypeTyped[T] with Encodeable {
  def apply(value: T): LValue = LValue(value, this)
}

