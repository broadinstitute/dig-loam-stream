package loamstream.model.piles

import loamstream.model.values.LType
import loamstream.model.values.LType.LTuple

import scala.language.existentials

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object LSig {

  case class Set(keyTypes: LTuple) extends LSig {
    override def =:=(oSig: LSig): Boolean = oSig match {
      case Set(oKeyTypes) => keyTypes == oKeyTypes
      case _ => false
    }
  }

  case class Map(keyTypes: LTuple, vType: LType[_]) extends LSig {
    override def =:=(oSig: LSig): Boolean =
      oSig match {
        case Map(oKeyTypes, oVType) => keyTypes == oKeyTypes && vType == oVType
        case _ => false
      }
  }

}

sealed trait LSig {
  def keyTypes: LTuple

  def =:=(oSig: LSig): Boolean
}
