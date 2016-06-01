package loamstream.util

import scala.reflect.runtime.universe.{TypeTag, typeOf, Symbol}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 5/16/16.
  */
object SourceUtils {

  private def typeSymbol[T : TypeTag]: Symbol = typeOf[T].typeSymbol
  
  def fullTypeName[T: TypeTag]: String = typeSymbol[T].fullName

  def shortTypeName[T: TypeTag]: String = typeSymbol[T].name.toString

}
