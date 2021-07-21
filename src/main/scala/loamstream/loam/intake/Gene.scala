package loamstream.loam.intake

/**
  * @author clint
  * @date Jul 8, 2021
  *
  */
final class Gene private (val value: String) {
  override def toString: String = value

  override def hashCode: Int = value.hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Gene => this.value == that.value
    case _ => false
  }
}

object Gene {
  def apply(s: String): Gene = {
    //TODO: Validate s actually refers to a known gene?
    new Gene(s)
  }
}