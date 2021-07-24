package loamstream.loam.intake

import org.apache.commons.csv.CSVRecord
import loamstream.loam.intake.flip.Disposition
import scala.util.Failure
import org.json4s._
import org.json4s.JsonAST.JNumber
import scala.collection.compat._

/**
 * @author clint
 * Feb 10, 2020
 */
trait DataRow extends KeyedRow with IndexedRow with RowWithRecordNumber with SkippableRow[DataRow]
 
object DataRow {
  final case class CommonsCsvDataRow(
    delegate: CSVRecord, 
    isSkipped: Boolean = false) extends DataRow {

    override def headers: Seq[String] = {
      import scala.collection.JavaConverters._
      
      delegate.getParser.getHeaderNames.iterator.asScala.toList
    }
    
    override def hasField(name: String): Boolean = delegate.isSet(name)
    
    override def getFieldByName(name: String): String = delegate.get(name)
    
    override def getFieldByIndex(i: Int): String = delegate.get(i)
    
    override def size: Int = delegate.size
    
    override def recordNumber: Long = delegate.getRecordNumber
    
    override def skip: CommonsCsvDataRow = copy(isSkipped = true)
  }
  
  final case class JsonDataRow(
    json: JObject, 
    recordNumber: Long, 
    isSkipped: Boolean = false) extends DataRow {

    override def headers: Seq[String] = json.values.keys.to(List)
    
    override def hasField(name: String): Boolean = json.obj.exists { case JField(n, _) => n == name } 
    
    override def getFieldByName(name: String): String = asString(json \ name, name)
    
    override def getFieldByNameOpt(name: String): Option[String] = asStringOpt(json \ name, name)
    
    override def getFieldByIndex(i: Int): String = asString(json.apply(i), i)
    
    override def size: Int = json.obj.size
    
    override def skip: JsonDataRow = copy(isSkipped = true)
    
    import org.json4s.jackson.JsonMethods._

    private def asString(jv: JValue, lookedFor: Any): String = jv match {
      case JsonDataRow.SimpleTypeAsString(s) => s
      case JNothing => {
        throw JsonProcessingException(s"Couldn't find key '${lookedFor}' in '${pretty(json)}'", json)
      }
      case jv => {
        throw JsonProcessingException(
            s"Value at '${lookedFor}' must be a boolean, number, or string in '${pretty(json)}' ", json)
      }
    }
    
    private def asStringOpt(jv: JValue, lookedFor: Any): Option[String] = jv match {
      case JsonDataRow.SimpleTypeAsString(s) => Option(s)
      case JNothing => None
      case jv => {
        throw JsonProcessingException(
            s"Value at '${lookedFor}' must be a boolean, number, or string in '${pretty(json)}' ", json)
      }
    }
  }
  
  object JsonDataRow {
    object SimpleTypeAsString {
      def unapply(jv: JValue): Option[String] = jv match {
        case JString(s) => Some(s)
        case n: JNumber =>  Some(n.values.toString)
        case JBool(b) => Some(b.toString)
        case _ => None
      }
    }
  }
}
