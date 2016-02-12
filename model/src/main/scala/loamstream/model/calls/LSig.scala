package loamstream.model.calls

import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
object LSig {

  object Set {
    def apply[Keys <: Product : TypeTag]: Shot[Set] = ProductTypeExploder.explode(typeTag[Keys].tpe).map(Set(_))
  }

  case class Set(keyTypes: Seq[Type]) extends LSig

  object Map {
    def apply[Keys <: Product : TypeTag, V: TypeTag]: Shot[Map] =
      ProductTypeExploder.explode(typeTag[Keys].tpe).map(Map(_, typeTag[V].tpe))
  }

  case class Map(keyTypes: Seq[Type], vType: Type) extends LSig

}

sealed trait LSig {
  def keyTypes: Seq[Type]
}
