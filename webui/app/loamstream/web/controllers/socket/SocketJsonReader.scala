package loamstream.web.controllers.socket

import loamstream.compiler.messages.{ClientInMessage, ListRequestMessage, LoadRequestMessage, SaveRequestMessage,
TextSubmitMessage}
import play.api.libs.json.{JsError, JsPath, JsResult, JsSuccess, JsValue, Reads}
import play.api.libs.functional.syntax.toFunctionalBuilderOps

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonReader extends Reads[ClientInMessage] {
  override def reads(json: JsValue): JsResult[ClientInMessage] = {
    val typeReads = (JsPath \ "type").read[String]
    json.validate(typeReads) match {
      case JsSuccess(typeName, _) => typeName match {
        case "text" =>
          val textSubmitMessageReads = (JsPath \ "text").read[String].map(TextSubmitMessage)
          json.validate(textSubmitMessageReads)
        case "load" =>
          val loadRequestMessageReads = (JsPath \ "name").read[String].map(LoadRequestMessage)
          json.validate(loadRequestMessageReads)
        case "list" =>
          JsSuccess(ListRequestMessage)
        case "save" =>
          val saveRequestMessageReads =
            ((JsPath \ "name").read[String] and (JsPath \ "content").read[String])(SaveRequestMessage)
          json.validate(saveRequestMessageReads)
        case _ => JsError(s"Don't know message type '$typeName'.")
      }
      case error: JsError => error
    }
  }
}
