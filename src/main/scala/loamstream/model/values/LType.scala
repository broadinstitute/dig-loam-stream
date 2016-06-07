package loamstream.model.values

import loamstream.model.LSig

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  case object LDouble extends LType

  case object LInt extends LTypeBuilder

  case object LString extends LTypeBuilder

  case object LGenotype extends LType

  sealed trait LTuple extends LTypeBuilder {
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

  def create[T: TypeTag]: LType = LTypeNative(typeTag[T].tpe)

}

trait LTypeBuilder extends LType {

  import LType.LTuple.{LTuple1, LTuple2}

  def &(other: LType): LTuple2 = LTuple2(this, other)

  def to(other: LType): LSig.Map = LTuple1(this) to other
}

case class LTypeNative(tpe: Type) extends LType

sealed trait LType {

}
