package loamstream.loam.intake.dga

import scala.util.Success
import scala.util.Try

import org.json4s.JArray
import org.json4s.JObject
import org.json4s.JString
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.render
import org.json4s.jvalue2monadic

import loamstream.util.Tries

/**
 * @author clint
 * Jan 20, 2021
 */
object Json {
  implicit final class JsonOps(val jv: JValue) extends AnyVal {
    private def makeMessage(tpe: String, fieldName: String): String = {
      s"Couldn't find ${tpe} field '${fieldName}' in ${compact(render(jv))}"
    }
    
    def tryAsString(fieldName: String): Try[String] = (jv \ fieldName) match {
      case JString(s) => Success(s.trim)
      case _ => Tries.failure(makeMessage("string", fieldName)) 
    }
    
    def tryAsStrings(fieldName: String): Try[Seq[String]] = tryAsArray(fieldName).map(_.map { case JString(s) => s })
    
    def asStringOption(fieldName: String): Option[String] = (jv \ fieldName) match {
      case JString(s) => Option(s.trim)
      case _ => None 
    }
    
    def tryAsArray(fieldName: String): Try[Seq[JValue]] = (jv \ fieldName) match {
      case JArray(jvs) => Success(jvs)
      case _ => Tries.failure(makeMessage("string", fieldName)) 
    }
    
    def tryAsStringArray(fieldName: String): Try[Seq[String]] = {
      tryAsArray(fieldName).map(_.collect { case JString(s) => s })
    }
    
    def tryAsObject(fieldName: String): Try[Map[String, JValue]] = (jv \ fieldName) match {
      case jobj: JObject => Success(jobj.obj.toMap)
      case _ => Tries.failure(makeMessage("string", fieldName)) 
    }
  }
}
