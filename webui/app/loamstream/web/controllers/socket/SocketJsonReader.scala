package loamstream.web.controllers.socket

import loamstream.compiler.{ClientInMessage, TextSubmitMessage}
import play.api.libs.json.{JsError, JsPath, JsResult, JsSuccess, JsValue, Reads}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonReader extends Reads[ClientInMessage]{
  override def reads(json: JsValue): JsResult[ClientInMessage] = {
    val typeReads = (JsPath \ "type").read[String]
    json.validate(typeReads) match {
      case JsSuccess(typeName, _) => typeName match {
        case "text" =>
          val textSubmitMessageReads = (JsPath \"text").read[String].map(TextSubmitMessage)
          json.validate(textSubmitMessageReads)
        case _ => JsError(s"Don't know message type '$typeName'.")
      }
      case error: JsError => error
    }
  }
}
