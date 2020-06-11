package loamstream.util

import scala.util.matching.Regex

/**
 * @author clint
 * Jul 8, 2019
 */
object Regexes {
  object Implicits {
    final implicit class RegexOps(val regex: Regex) extends AnyVal {
      def matchesAnywhere(input: String): Boolean = regex.pattern.matcher(input).find
    
      def doesntMatchAnywhere(input: String): Boolean = !matchesAnywhere(input)
    }
  }
}
