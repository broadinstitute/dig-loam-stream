package loamstream.model.piles

import loamstream.util.ProductTypeExploder

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object LSig {

  object Set {
    def apply[Keys: TypeTag]: Set = Set(ProductTypeExploder.explode(typeTag[Keys].tpe).map(new LType(_)))
  }

  case class Set(keyTypes: Seq[LType]) extends LSig {
    override def =:=(oSig: LSig): Boolean = oSig match {
      case Set(oKeyTypes) => keyTypes == oKeyTypes
      case _ => false
    }
  }

  object Map {
    def apply[Keys: TypeTag, V: TypeTag]: Map =
      Map(ProductTypeExploder.explode(typeTag[Keys].tpe).map(new LType(_)), new LType(typeTag[V].tpe))
  }

  case class Map(keyTypes: Seq[LType], vType: LType) extends LSig {
    override def =:=(oSig: LSig): Boolean =
      oSig match {
        case Map(oKeyTypes, oVType) => keyTypes == oKeyTypes && vType == oVType
        case _ => false
      }
  }

}

sealed trait LSig {
  def keyTypes: Seq[LType]

  def =:=(oSig: LSig): Boolean
}
