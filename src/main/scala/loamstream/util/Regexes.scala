package loamstream.util

import scala.util.matching.Regex

/**
 * @author clint
 * Jul 8, 2019
 */
object Regexes {
  object Implicits {
    final implicit class RegexOps(val regex: Regex) extends AnyVal {
      def foundIn(input: String): Boolean = regex.findFirstIn(input).isDefined
    
      def notFoundIn(input: String): Boolean = !foundIn(input)
    }
  }
}
