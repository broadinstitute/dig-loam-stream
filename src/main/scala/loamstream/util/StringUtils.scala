package loamstream.util

import java.nio.charset.StandardCharsets

import scala.util.Try
import scala.annotation.tailrec

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

  /** Regex matching line breaks on almost all platforms */
  val lineBreaksRegex =
  """\n\r|\r\n|\n|\r"""

  /** Replaces line breaks from almost all platforms to line breaks of the current platform */
  def assimilateLineBreaks(string: String): String = string.replaceAll(lineBreaksRegex, System.lineSeparator)

  /** Replaces each line break by a space on almost all platforms.  */
  def unwrapLines(string: String): String = string.replaceAll(lineBreaksRegex, " ")

  /** Converts backslashes into double backslashes */
  def unescapeBackslashes(string: String): String = string.replaceAll("\\\\", "\\\\\\\\")

  def fromUtf8Bytes(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)

  def leftPadTo(string: String, pad: String, minLength: Int): String = {
    if (string.length < minLength) {
      val padLength = minLength - string.length
      pad * padLength + string
    } else {
      string
    }
  }
  
  /** Collapse sequences of whitepace in a string into single spaces */
  def collapseWhitespace(s: String): String = {
    def toSpace(c: Char): Char = if(c.isWhitespace) ' ' else c
    
    @tailrec
    def loop(remaining: Seq[Char], onWsStreak: Boolean, acc: StringBuilder): String = {
      remaining match {
        //No more chars to process, return the accumulator
        case Nil => acc.toString
        case first +: rest => onWsStreak match {
          //We're not on a ws streak
          case false => loop(rest, first.isWhitespace, acc += toSpace(first))
          //Skip subsequent ws (don't add it to acc) if we're on a streak
          case true if first.isWhitespace => loop(rest, onWsStreak = true, acc)
          //Seeing non-ws after ws means the streak is broken
          case true => loop(rest, onWsStreak = false, acc += toSpace(first))
        }
      }
    }
    
    loop(s.toSeq, onWsStreak = false, new StringBuilder)
  }
}
