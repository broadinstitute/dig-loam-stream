package loamstream.model.values

import scala.reflect.runtime.universe.{Type, typeOf}

/**
  * LoamStream
  * Created by oliverr on 6/6/2016.
  */
object LTypeAnalyzer {

  // TODO: this would break if some one creates sub-types of Map with unusual parametrization
  def isMap(tpe: Type): Boolean =
    tpe.erasure <:< typeOf[scala.collection.Map[_, _]].erasure && tpe.typeArgs.size == 2

  // TODO: this would break if some one creates sub-types of Iterable with unusual parametrization
  def isIterable(tpe: Type): Boolean =
    tpe.erasure <:< typeOf[scala.collection.Iterable[_]].erasure && tpe.typeArgs.size == 1

  def isProduct(tpe: Type): Boolean = tpe <:< typeOf[scala.Product]

  def explode(tpe: Type): Seq[Type] = if (isProduct(tpe)) {
    tpe.typeArgs
  } else {
    Seq(tpe)
  }

  def keyTypes(tpe: Type): Seq[Type] = {
    if (isMap(tpe) || isIterable(tpe)) {
      explode(tpe.typeArgs.head)
    } else {
      Seq.empty
    }
  }

}
