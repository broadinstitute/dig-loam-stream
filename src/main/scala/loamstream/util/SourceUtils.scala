package loamstream.util

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

/** Methods for source code creation */
object SourceUtils {
  /** Returns full type name, such as collection.immutable.Set */
  def fullTypeName(tpe: Type): String = tpe.typeSymbol.fullName

  /** Returns full type name, such as collection.immutable.Set */
  def fullTypeName[T: TypeTag]: String = fullTypeName(typeOf[T])

  /** Returns short type name, such as Set */
  def shortTypeName(tpe: Type): String = tpe.typeSymbol.name.toString

  /** Returns short type name, such as Set */
  def shortTypeName[T: TypeTag]: String = shortTypeName(typeOf[T])

}
