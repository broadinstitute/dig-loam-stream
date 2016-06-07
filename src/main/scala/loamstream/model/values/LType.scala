package loamstream.model.values

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  sealed trait LTuple {
    def asSeq: Seq[LType]

    override def toString: String = asSeq.mkString(" & ")

  }

  object LTuple {

    case class LTupleN(types: Seq[Type]) extends LTuple {
      override def asSeq: Seq[LType] = types.map(LTypeNative)
    }

  }

  def create[T: TypeTag]: LType = LTypeNative(typeTag[T].tpe)

}

case class LTypeNative(tpe: Type) extends LType

sealed trait LType {

}
