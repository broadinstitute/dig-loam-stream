package loamstream.model.values

import loamstream.model.LSig

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

// scalastyle:off
object LType {

  case object LBoolean extends LType

  case object LDouble extends LType

  case object LFloat extends LType

  case object LLong extends LType

  case object LInt extends LType

  case object LShort extends LType

  case object LChar extends LType

  case object LByte extends LType

  case object LString extends LType

  case object LGenotype extends LType

  sealed trait LIterable extends LType

  object LSet {
    def apply(elementType: LType): LSet = LSetAny(elementType)
  }

  sealed trait LSet extends LIterable

  case class LSetAny(elementType: LType) extends LSet

  object LSeq {
    def apply(elementType: LType): LSeq = LSeqAny(elementType)
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

sealed trait LType {

  import LType.LTuple.{LTuple1, LTuple2}

  def &(other: LType): LTuple2 = LTuple2(this, other)

  def to(other: LType): LSig.Map = LTuple1(this) to other
}

//scalastyle:on
