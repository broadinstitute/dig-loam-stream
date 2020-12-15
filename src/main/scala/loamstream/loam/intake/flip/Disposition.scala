package loamstream.loam.intake.flip

/**
 * @author clint
 * Oct 21, 2020
 */
sealed abstract class Disposition(val isFlipped: Boolean, val isSameStrand: Boolean) {
  final def notFlipped: Boolean = !isFlipped
  final def isComplementStrand: Boolean = !isSameStrand
}

object Disposition {
  case object NotFlippedComplementStrand extends Disposition(isFlipped = false, isSameStrand = false)
  case object FlippedComplementStrand extends Disposition(isFlipped = true, isSameStrand = false)
  case object NotFlippedSameStrand extends Disposition(isFlipped = false, isSameStrand = true)
  case object FlippedSameStrand extends Disposition(isFlipped = true, isSameStrand = true)
}
