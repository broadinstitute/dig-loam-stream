package loamstream.util

import java.nio.file.{Path, Paths}

import scala.util.Try
import java.nio.charset.StandardCharsets

/**
  * Created on: 3/1/16
  *
  * @author Kaan Yuksel
  */
object StringUtils {

  def isWhitespace(s: String): Boolean = s.trim.isEmpty

  def isNotWhitespace(s: String): Boolean = !isWhitespace(s)

  object IsLong {
    def unapply(s: String): Option[Long] = Try(s.toLong).toOption
  }

  val numNames =
    Seq("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve")

  /** Converts numbers to Strings while using number words for number zero to twelve. */
  def prettyPrint(num: Int): String = if (num < numNames.size) numNames(num) else num.toString

  /** Prints expression specifying a count of some item, assuming plural is singular plus s. */
  def soMany(count: Int, singular: String): String = soMany(count, singular, singular + "s")

  /** Prints expression specifying a count of some item such as 'five apples' */
  def soMany(count: Int, singular: String, plural: String): String = count match {
    case 0 => "no " + plural
    case 1 => "one " + singular
    case _ => s"${prettyPrint(count)} $plural"
  }

  /** Replaces each line break by a space on almost all platforms.  */
  def unwrapLines(string: String): String =
    string.replaceAll("\r\n", " ").replaceAll("\n\r", " ").replaceAll("\n", " ").replaceAll("\r", " ")

  /** Converts backslashes into double backslashes */
  def unescapeBackslashes(string: String): String = string.replaceAll("\\\\", "\\\\\\\\")
  
  def fromUtf8Bytes(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8) 
}
