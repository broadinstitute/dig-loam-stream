package loamstream.util

import scala.util.matching.Regex

/**
 * @author clint
 * Jul 8, 2019
 */
object Regexes {
  object Implicits {
    final implicit class RegexOps(val regex: Regex) extends AnyVal {
      def matches(input: String): Boolean = regex.findFirstMatchIn(input).isDefined
    
      def doesntMatch(input: String): Boolean = !matches(input)
    }
  }
}
