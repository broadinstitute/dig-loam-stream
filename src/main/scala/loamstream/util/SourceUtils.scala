package loamstream.util

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 5/16/16.
  */
object SourceUtils {

  def fullTypeName(tpe: Type): String = tpe.typeSymbol.fullName

  def fullTypeName[T: TypeTag]: String = fullTypeName(typeOf[T])

  def shortTypeName(tpe: Type): String = tpe.typeSymbol.name.toString

  def shortTypeName[T: TypeTag]: String = shortTypeName(typeOf[T])
}
