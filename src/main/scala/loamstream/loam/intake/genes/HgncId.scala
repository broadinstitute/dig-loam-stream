package loamstream.loam.intake.genes

import loamstream.util.ValueBox

/**
  * @author clint
  * @date Jul 19, 2021
  *
  * 
  */
final class HgncId private (val value: String) extends AnyVal

object HgncId extends Interned.Companion[String, HgncId, Map](new HgncId(_)) {
  override def apply(s: String): HgncId = super.apply(s.trim) //TODO: More than trim?
}