package loamstream.loam.intake

/**
 * @author clint
 * Oct 27, 2020
 */
final case class Dataset(name: String) extends HasName

final case class Phenotype(name: String, dichotomous: Boolean) extends HasName
