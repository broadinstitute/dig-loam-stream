package loamstream.loam.intake.dga

import loamstream.loam.intake.RenderableRow
import loamstream.loam.intake.RenderableJsonRow
import org.json4s.JsonAST.JValue

/**
 * @author clint
 * Dec 1, 2020
 */
final case class Tissue(id: Option[String], name: Option[String]) extends RenderableJsonRow {
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
  
  override def jsonValues: Seq[(String, JValue)] = {
    import Json.toJValue
    
    Seq("id" -> toJValue(id), "name" -> toJValue(name))
  }
}
