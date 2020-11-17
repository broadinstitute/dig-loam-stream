package loamstream.loam.intake

/**
 * @author clint
 * Nov 16, 2020
 */
object Chromosomes {
  val names: Iterable[String] = ((1 to 22).map(_.toString) ++ Seq("X", "Y", "XY", "MT")).toSet 
}
