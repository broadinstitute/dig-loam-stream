package loamstream.loam.intake.dga

import loamstream.util.HttpClient
import loamstream.util.SttpHttpClient
import org.json4s._
import org.json4s.JsonAST.JNumber
import loamstream.loam.intake.Source

/**
 * @author clint
 * Dec 1, 2020
 */
trait DgaSyntax { 
  object Dga {
    def versionAndTissueSource(
        httpClient: HttpClient = new SttpHttpClient(), 
        url: String = Defaults.tissueUrl): (String, Source[Tissue]) = {
      
      import org.json4s.jackson.JsonMethods._
      
      def getTissueRows(json: JValue): Iterable[JObject] = {
        (json \ "tissues") match {
          case JArray(os) => os.collect { case o: JObject => o }
          case _ => {
            val first1000 = compact(render(json)).take(1000)
            
            sys.error(s"Couldn't parse 'tissues' array; first 1k characters follows: '${first1000}'")
          }
        }
      }
      
      def getVersion(json: JValue): String = (json \ "version") match {
        case JString(v) => v
        case n: JNumber => n.values.toString
        case jv => sys.error(s"Couldn't determine tissue ontology version, got $jv from '${pretty(json)}'")
      }
      
      val jsonE = httpClient.post(url, headers = Map("Content-Type" -> "application/json")).map(parse(_))
      
      /*val (version, rows) = jsonE match {
        case Right(json) => getVersion(json) -> Source.fromJson(getTissueRows) { 
          ???
        }
        case Left(message) => sys.error(s"Error accessing tissue ontology: '${message}'")
      }
      
      def toTissue(row: DataRow): Tissue = {
        val tid = ColumnName("tissue_id")
        val name = ColumnName("name")
        
        Tissue(id = row.getFieldByNameOpt("tissue_id"), name = row.getFieldByNameOpt("name")) 
      }
      
      (version.trim, rows.map(toTissue))*/
      
      ???
    }
    
    object Defaults {
      val tissueUrl: String = "http://www.diabetesepigenome.org:8080/tissueOntology"
    }
  }
}
