package loamstream.model

import loamstream.model.values.LType
import loamstream.model.values.LType.LTuple
import scala.language.existentials

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object LSig {

  case class Set(keyTypes: LTuple[_ <: Product]) extends LSig {
    
    override def toString: String = keyTypes.toString
    
    override def =:=(oSig: LSig): Boolean = oSig match {
      case Set(oKeyTypes) => keyTypes == oKeyTypes
      case _ => false
    }
  }
  
  object Set {
    def of[K](k: LType[K]): LSig.Set = new LSig.Set(LTuple.LTuple1(k))
  }

  case class Map(keyTypes: LTuple[_ <: Product], vType: LType[_]) extends LSig {
    
    override def toString: String = s"$keyTypes to $vType"
    
    override def =:=(oSig: LSig): Boolean =
      oSig match {
        case Map(oKeyTypes, oVType) => keyTypes == oKeyTypes && vType == oVType
        case _ => false
      }
  }

}

sealed trait LSig {
  def keyTypes: LTuple[_]

  def =:=(oSig: LSig): Boolean
}
