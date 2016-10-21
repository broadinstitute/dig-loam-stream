package loamstream.util

import java.util.regex.{Matcher, Pattern}

/** A simple templating device, substituting delimited keys in a String with values (properties) */
case class Templater(prefix: String, suffix: String, props: Map[String, String]) {
  /** Returns templater with that property added */
  def withProp(key: String, value: String): Templater = copy(props = props + (key -> value))

  /** Returns templater with those property added */
  def withProps(oProps: Map[String, String]): Templater = copy(props = props ++ oProps)

  /** Returns templater with that property added */
  def +(key: String, value: String): Templater = withProp(key, value)

  /** Returns templater with those property added */
  def ++(oProps: Map[String, String]): Templater = withProps(oProps)

  /** Substitute delimited keys in that template String */
  def apply(template: String): String = {
    var result = template
    for ((key, value) <- props) {
      val pattern = Pattern.quote(prefix + key + suffix)
      result = result.replaceAll(pattern, Matcher.quoteReplacement(value))
    }
    result
  }
}

/** A simple templating device  */
object Templater {
  /** Templater with that prefix and suffix */
  def apply(prefix: String, suffix: String): Templater = Templater(prefix, suffix, Map.empty)

  /** Templater using double curly braces as key delimiters, like the Moustache library */
  val moustache: Templater = apply("{{", "}}")
}