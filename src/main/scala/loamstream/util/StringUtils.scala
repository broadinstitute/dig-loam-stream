package loamstream.util

import java.nio.file.{Path, Paths}

import scala.util.Try

/**
  * Created on: 3/1/16
  *
  * @author Kaan Yuksel
  */
object StringUtils {
  //TODO: TEST!
  def pathTemplate(template: String, slot: String): String => Path = {
    id => Paths.get(template.replace(slot, id))
  }

  def isWhitespace(s: String): Boolean = s.trim.isEmpty

  def isNotWhitespace(s: String): Boolean = !isWhitespace(s)

  object IsLong {
    def unapply(s: String): Option[Long] = Try(s.toLong).toOption
  }

  val numNames =
    Seq("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve")

  def prettyPrint(num: Int): String = if (num < numNames.size) numNames(num) else num.toString

  def soMany(count: Int, singular: String): String = soMany(count, singular, singular + "s")

  def soMany(count: Int, singular: String, plural: String): String = count match {
    case 0 => "no " + plural
    case 1 => "one " + singular
    case _ => s"${prettyPrint(count)} $plural"
  }
}
