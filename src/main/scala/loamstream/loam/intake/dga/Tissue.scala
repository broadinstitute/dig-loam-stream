package loamstream.loam.intake.dga

import loamstream.loam.intake.RenderableRow

/**
 * @author clint
 * Dec 1, 2020
 */
final case class Tissue(id: Option[String], name: Option[String]) extends RenderableRow {
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
  
  override def headers: Seq[String] = Seq("id", "name")
  
  override def values: Seq[Option[String]] = Seq(id, name)
}
