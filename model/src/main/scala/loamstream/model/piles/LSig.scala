package loamstream.model.piles

import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 2/12/2016.
 */
object LSig {

  def areEquivalent(types1: Seq[Type], types2: Seq[Type]): Boolean =
    types1.zip(types2).map(tup => tup._1 =:= tup._2).forall(b => b)

  object Set {
    def apply[Keys <: Product : TypeTag]: Shot[Set] = ProductTypeExploder.explode(typeTag[Keys].tpe).map(Set(_))
  }

  case class Set(keyTypes: Seq[Type]) extends LSig {
    override def =:=(oSig: LSig): Boolean = oSig match {
      case Set(oKeyTypes) => areEquivalent(keyTypes, oKeyTypes)
      case _ => false
    }
  }

  object Map {
    def apply[Keys <: Product : TypeTag, V: TypeTag]: Shot[Map] =
      ProductTypeExploder.explode(typeTag[Keys].tpe).map(Map(_, typeTag[V].tpe))
  }

  case class Map(keyTypes: Seq[Type], vType: Type) extends LSig {
    override def =:=(oSig: LSig): Boolean =
      oSig match {
        case Map(oKeyTypes, oVType) => areEquivalent(keyTypes, oKeyTypes) && vType =:= oVType
      }
  }

}

sealed trait LSig {
  def keyTypes: Seq[Type]

  def =:=(oSig: LSig): Boolean
}
