package loamstream.loam.intake.flip

/**
 * @author clint
 * Oct 20, 2020
 */
sealed trait FlipStatus

object FlipStatus {
  case object Flipped extends FlipStatus
  case object NotFlipped extends FlipStatus
}
