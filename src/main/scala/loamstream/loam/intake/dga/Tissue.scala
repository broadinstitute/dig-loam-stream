package loamstream.loam.intake.dga

/**
 * @author clint
 * Dec 1, 2020
 */
final case class Tissue(id: Option[String], name: Option[String]) {
  def isValid: Boolean = {
    def isValidPrefix(s: String): Boolean = s.trim match {
      case "UBERON" | "CLO" | "CL" | "EFO" | "BTO" => true
      case _ => false
    }
    
    id.flatMap(_.split(":").headOption) match {
      case Some(prefix) => isValidPrefix(prefix)
      case _ => false
    }
  }
}
