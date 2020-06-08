package loamstream.loam.intake.metrics

import org.json4s.jackson.JsonMethods.parse
import org.json4s._
import scala.util.Try

import SttpBioindexClient.Defaults

import loamstream.util.Loggable

import sttp.client.HttpURLConnectionBackend
import loamstream.loam.intake.Variant


/**
 * @author clint
 * Mar 24, 2020
 */
//http://ec2-18-215-38-136.compute-1.amazonaws.com:5000/api/query/Variants?q=chr8:100000-101000
final class SttpBioindexClient(baseUrl: String = Defaults.baseUrl) extends BioIndexClient with Loggable {
  override def isKnown(varId: String): Boolean = isKnown(Variant.from(varId))
  
  private val backend = HttpURLConnectionBackend()
  
  def isKnown(variant: Variant): Boolean = {
    import sttp.client._
    import SttpBioindexClient.{anyResults, extractVarIds}
    
    debug(s"Looking up ${variant.colonDelimited}")
    
    val url = uri"${baseUrl}?q=${variant.asBioIndexCoord}"
    
    trace(s"Invoking $url")
    
    val request = basicRequest.get(url)
    
    val responseBody = backend.send(request).body 
    
    responseBody match {
      case Left(errorMessage) => sys.error(errorMessage)
      case Right(contents) => {
        val parsed = parse(contents) 
        
        anyResults(parsed, contents) && {
          val varIds = extractVarIds(parsed, variant, contents)
          
          varIds.exists(_ == variant.colonDelimited)
        }
      }
    }
  }
}

object SttpBioindexClient {
  object Defaults {
    val baseUrl: String = "http://ec2-18-215-38-136.compute-1.amazonaws.com:5000/api/query/Variants"
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