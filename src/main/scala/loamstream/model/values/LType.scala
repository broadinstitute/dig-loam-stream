package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig
import loamstream.model.Types.{ClusterId, SampleId, SingletonCount, VariantId}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

// scalastyle:off
object LType {

  case object LBoolean extends LTypeAtomic[Boolean]

  case object LDouble extends LTypeAtomic[Double]

  case object LFloat extends LTypeAtomic[Float]

  case object LLong extends LTypeAtomic[Long]

  case object LInt extends LTypeAtomic[Int]

  case object LShort extends LTypeAtomic[Short]

  case object LChar extends LTypeAtomic[Char]

  case object LByte extends LTypeAtomic[Byte]

  case object LString extends LTypeAtomic[String]

  case object LVariantId extends LTypeAtomic[VariantId]

  case object LSampleId extends LTypeAtomic[SampleId]

  case object LGenotype extends LTypeAtomic[Genotype]

  case object LSingletonCount extends LTypeAtomic[SingletonCount]

  case object LClusterId extends LTypeAtomic[ClusterId]

  sealed trait LIterable extends LType {
    def elementType: LType
  }

  object LSet {
    def apply(elementType: LType): LSet = LSetAny(elementType)

    def unapply(set: LSet): Option[LType] = Some(set.elementType)
  }

  sealed trait LSet extends LIterable

  case class LSetAny(elementType: LType) extends LSet

  object LSeq {
    def apply(elementType: LType): LSeq = LSeqAny(elementType)

    def unapply(seq: LSeq): Option[LType] = Some(seq.elementType)
  }

  sealed trait LSeq extends LIterable {
  }

  case class LSeqAny(elementType: LType) extends LSeq

  sealed trait LTuple extends LType {
    def asSeq: Seq[LType]

    override def to(v: LType): LSig.Map = LSig.Map(this, v)

    override def toString: String = asSeq.mkString(" & ")

  }

  object LTuple {

    case class LTuple1(type1: LType) extends LTuple {
      override def asSeq: Seq[LType] = Seq(type1)
    }

    case class LTuple2(type1: LType, type2: LType) extends LTuple {
      override def asSeq: Seq[LType] = Seq(type1, type2)
    }

  }

}

sealed trait LTypeAtomic[T] extends LType {
  def apply(value: T): LValue = LValue(value, this)

  def of(value: T): LValue = apply(value)
}

sealed trait LType {
  def toValue(value: Any): LValue = LValue(value, this)

  import LType.LTuple.{LTuple1, LTuple2}

  def &(other: LType): LTuple2 = LTuple2(this, other)

  def to(other: LType): LSig.Map = LTuple1(this) to other
}

//scalastyle:on
