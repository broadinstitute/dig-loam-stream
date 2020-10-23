package loamstream.loam.intake.flip

/**
 * @author clint
 * Oct 20, 2020
 */
sealed trait ComplementStatus

object ComplementStatus {
  case object Complemented extends ComplementStatus
  case object NotComplemented extends ComplementStatus
}
