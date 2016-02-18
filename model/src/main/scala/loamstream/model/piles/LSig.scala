package loamstream.model.piles

import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 2/12/2016.
 */
object LSig {

  object Set {
    def apply[Keys <: Product : TypeTag]: Shot[Set] =
      ProductTypeExploder.explode(typeTag[Keys].tpe).map(types => Set(types.map(new LType(_))))
  }

  case class Set(keyTypes: Seq[LType]) extends LSig {
    override def =:=(oSig: LSig): Boolean = oSig match {
      case Set(oKeyTypes) => keyTypes == oKeyTypes
      case _ => false
    }
  }

  object Map {
    def apply[Keys <: Product : TypeTag, V: TypeTag]: Shot[Map] =
      ProductTypeExploder.explode(typeTag[Keys].tpe)
        .map(types => Map(types.map(new LType(_)), new LType(typeTag[V].tpe)))
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
