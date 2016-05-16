package loamstream.util

import scala.reflect.runtime.universe.{typeTag, TypeTag}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 5/16/16.
  */
object SourceUtils {

  def likelyTypeName(obj: AnyRef): String = likelyTypeName(obj.getClass)

  def likelyTypeName(clazz: Class[_]): String = likelyTypeName(clazz.getName)

  def likelyTypeName[T: TypeTag]: String = likelyTypeName(typeTag[T].tpe.toString)

  def likelyTypeName(rawName: String): String = {
    val nameNoTrailing = if (rawName.endsWith("$")) rawName.dropRight(1) else rawName
    nameNoTrailing.replace("$", ".")
  }

}
