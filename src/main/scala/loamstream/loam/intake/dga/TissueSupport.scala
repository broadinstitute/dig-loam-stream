package loamstream.loam.intake.dga

import org.json4s.JsonAST.JArray
import org.json4s.JsonAST.JNumber
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JString
import org.json4s.JsonAST.JValue

import loamstream.loam.intake.ColumnName
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Source
import loamstream.util.HttpClient
import loamstream.util.Loggable
import loamstream.util.SttpHttpClient
import loamstream.util.TimeUtils
import org.json4s.jackson.JsonMethods._

/**
 * @author clint
 * Feb 1, 2021
 */
trait TissueSupport { self: Loggable =>
  object Tissues {
    def versionAndTissueSource(jsonData: => String): (Source[String], Source[Tissue]) = {
      
      def getVersion(json: JValue): String = (json \ "version") match {
        case JString(v) => v
        case n: JNumber => n.values.toString
        case jv => sys.error(s"Couldn't determine tissue ontology version, got $jv from '${pretty(json)}'")
      }
      
      def toTissue(row: DataRow): Tissue = {
        val tid = ColumnName("tissue_id")
        val name = ColumnName("name")
        
        Tissue(id = row.getFieldByNameOpt("tissue_id"), name = row.getFieldByNameOpt("name")) 
      }
      
      def getTissueRows(json: JValue): Iterable[JObject] = {
        (json \ "tissues") match {
          case JArray(os) => os.collect { case o: JObject => o }
          case _ => {
            import Defaults.numTissuesToPrintWhenLogging
            
            val firstN = compact(render(json)).take(numTissuesToPrintWhenLogging)
            
            sys.error { 
              s"Couldn't parse 'tissues' array; first ${numTissuesToPrintWhenLogging} characters follows: '${firstN}'"
            }
          }
        }
      }
      
      val json = parse(jsonData)
      
      val version = Source.producing {
        getVersion(json).trim
      }
        
      val rows = Source.fromJson(getTissueRows)(json).map(toTissue)

      version -> rows
    }
    
    def versionAndTissueSource(
          url: String = Defaults.tissueUrl,
          httpClient: HttpClient = new SttpHttpClient()): (Source[String], Source[Tissue]) = {
      
      def getJson: String = TimeUtils.time(s"Hitting $url", info(_)) {
        httpClient.post(url, headers = Headers.ContentType.applicationJson) match {
          case Right(json) => json
          case Left(message) => sys.error(s"Error accessing tissue ontology: '${message}'")
        }
      }
      
      versionAndTissueSource(getJson)
    }
    
    object Defaults {
      val tissueUrl: String = "http://www.diabetesepigenome.org:8080/tissueOntology"
      
      val numTissuesToPrintWhenLogging: Int = 1000
    }
  }
}
