package loamstream.model

import loamstream.model.values.LType.LTuple
import loamstream.model.values.LType.LTuple.LTupleN
import loamstream.model.values.{LType, LTypeAnalyzer}

import scala.language.existentials
import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}


/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object LSig {

  case class Map(keyTypes: LTuple, vType: LType) extends LSig {

    override def toString: String = s"$keyTypes to $vType"

    override def =:=(oSig: LSig): Boolean =
      oSig match {
        case Map(oKeyTypes, oVType) => keyTypes == oKeyTypes && vType == oVType
        case _ => false
      }
  }

  def create[T: TypeTag]: LSigNative = LSigNative(typeTag[T].tpe)

  case class LSigNative(tpe: Type) extends LSig {
    override def keyTypes: LTuple = LTupleN(LTypeAnalyzer.keyTypes(tpe))

    override def =:=(oSig: LSig): Boolean = oSig match {
      case LSigNative(oTpe) => tpe =:= oTpe
      case _ => false
    }
  }

}

sealed trait LSig {
  def keyTypes: LTuple

  def =:=(oSig: LSig): Boolean
}
