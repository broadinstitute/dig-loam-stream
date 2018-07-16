package loamstream.cli

import scala.util.matching.Regex

/**
 * @author clint
 * Jul 2, 2018
 */
sealed trait JobFilterIntent

object JobFilterIntent {
  case object RunEverything extends JobFilterIntent
  case object DontFilterByName extends JobFilterIntent
  
  final case class RunIfAllMatch(regexes: Seq[Regex]) extends JobFilterIntent
  final case class RunIfAnyMatch(regexes: Seq[Regex]) extends JobFilterIntent
  final case class RunIfNoneMatch(regexes: Seq[Regex]) extends JobFilterIntent
}
