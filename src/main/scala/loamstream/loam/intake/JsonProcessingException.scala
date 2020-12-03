package loamstream.loam.intake

import org.json4s.JsonAST.JObject

/**
 * @author clint
 * Jul 20, 2020
 */
final case class JsonProcessingException(message: String, json: JObject, cause: Throwable = null) extends 
    Exception(message, cause)
