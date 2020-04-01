package loamstream.loam.intake.metrics

import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import scala.util.Try
import loamstream.loam.intake.Variant


/**
 * @author clint
 * Mar 31, 2020
 */
object BioIndexResultParser {
  def containsVariant(variant: Variant, json: String): Either[String, Boolean] = {
    
    val parsedE: Either[String, JValue] = Try(parse(json)).toEither.left.map(_.getMessage)
        
    parsedE.map { parsed =>
      anyResults(parsed, json) && {
        val varIds = extractVarIds(parsed, variant, json)
        
        varIds.exists(_ == variant.colonDelimited)
      }
    }
  }
  
  private def anyResults(parsed: JValue, contents: String): Boolean = {
    (parsed \ Names.count) match {
      case JInt(count) => count > 0
      case _ => sys.error(s"Couldn't parse 'count' field from JSON: '${contents}'")
    }
  }
  
  private def extractVarIds(parsed: JValue, variant: Variant, contents: String): Iterator[String] = {
    (parsed \ Names.data) match {
      case AllVarIdsFromDataElems(varIds) => varIds 
      case AllVarIdsFromAllAssociations(varIds) => varIds
      case _ => {
        sys.error(s"Error looking up ${variant.colonDelimited}: couldn't extract varIds from JSON: '$contents'")
      }
    }
  }
  
  object AllVarIdsFromAllAssociations {
    def unapply(data: JValue): Option[Iterator[String]] = data match {
      case JArray(dataElems) => {
        val varIds = dataElems.iterator.collect {
          case VarIdsFromAssociations(varIdChunk) => varIdChunk
        }.flatten
        
        if(varIds.hasNext) Some(varIds) else None
      }
      case _ => None
    }
  }
  
  object AllVarIdsFromDataElems {
    def unapply(data: JValue): Option[Iterator[String]] = data match {
      case JArray(dataElems) => {
        val varIds = dataElems.collect { case VarIdFromDataElem(varId) => varId }
        
        if(varIds.nonEmpty) Some(varIds.iterator) else None
      }
      case _ => None
    }
  }
  
  object VarIdFromDataElem {
    def unapply(dataElem: JValue): Option[String] = (dataElem \ Names.varId) match {
      case JString(varId) => Some(varId)
      case _ => None
    }
  }
    
  object VarIdsFromAssociations {
    def unapply(dataElem: JValue): Option[Iterator[String]] = (dataElem \ Names.associations) match {
      case JArray(associations) => Some {
        associations.iterator.flatMap { assoc =>
          (assoc \ Names.varId) match {
            case JString(varId) => Some(varId)
            case _ => None
          }
        }
      }
      case _ => None
    }
  }
  
  private object Names {
    val varId = "varId"
    val associations = "associations"
    val data = "data"
    val count = "count"
  }
}
