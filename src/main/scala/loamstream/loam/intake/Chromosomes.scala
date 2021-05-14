package loamstream.loam.intake

import scala.collection.compat._

/**
 * @author clint
 * Nov 16, 2020
 */
object Chromosomes {
  val names: Iterable[String] = (1 to 22).map(_.toString).to(Set) ++ Set("X", "Y", "XY", "MT") 
}
