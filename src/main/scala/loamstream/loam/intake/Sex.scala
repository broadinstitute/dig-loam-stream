package loamstream.loam.intake

/**
 * @author psmadbec
 *
 * 9 Feb, 2022
 */
sealed abstract class Sex(val name: String) {
  def unapply(s: String): Boolean = s.toLowerCase == name.toLowerCase

  override def toString: String = name.toLowerCase
}

object Sex {
  case object Male extends Sex("male")
  case object Female extends Sex("female")
  case object Mixed extends Sex("mixed")

  val values: Set[Sex] = Set(
    Male,
    Female,
    Mixed)

  private lazy val namesToValues: Map[String, Sex] = values.iterator.map(g => g.toString -> g).toMap

  def fromString(s: String): Option[Sex] = {
    val normalized = s.toLowerCase

    namesToValues.get(normalized)
  }
}
