package loamstream.util

import scala.reflect.runtime.universe.{typeOf, TypeTag}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 5/16/16.
  */
object SourceUtils {

  def fullTypeName[T: TypeTag]: String = typeOf[T].typeSymbol.fullName

}
