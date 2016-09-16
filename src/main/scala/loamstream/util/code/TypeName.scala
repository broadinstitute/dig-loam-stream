package loamstream.util.code

import scala.reflect.NameTransformer

/**
  * LoamStream
  * Created by oliverr on 9/15/2016.
  */
object TypeName {
  /** Returns true if name needs backticks in source code */
  def needsBackticks(name: String): Boolean = name != NameTransformer.encode(name)

  /** Wraps into backticks if needed to appear in code */
  def withBackticksIfNeeded(name: String): String =
  if (needsBackticks(name)) {
    s"`$name`"
  } else {
    name
  }

  def apply(part: String, parts: String*): TypeName = TypeName(part +: parts)
}

case class TypeName(parts: Seq[String]) {
  def shortNameScala: String = TypeName.withBackticksIfNeeded(parts.last)

  def fullNameScala: String = parts.map(TypeName.withBackticksIfNeeded).mkString(".")

  def shortNameJvm: String = NameTransformer.encode(parts.last)

  def fullNameJvm: String = parts.map(NameTransformer.encode).mkString(".")

  def +(part: String): TypeName = TypeName(parts :+ part)
}

