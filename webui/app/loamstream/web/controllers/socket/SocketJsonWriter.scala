package loamstream.web.controllers.socket

import loamstream.compiler.messages.{ClientOutMessage, ListResponseMessage, LoadResponseMessage, SaveResponseMessage}
import play.api.libs.json.{JsValue, Json, Writes}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonWriter extends Writes[ClientOutMessage] {
  override def writes(clientOutMessage: ClientOutMessage): JsValue = clientOutMessage match {
    case LoadResponseMessage(name, content, message) =>
      Json.obj(
        "type" -> clientOutMessage.typeName,
        "message" -> message,
        "content" -> content
      )
    case ListResponseMessage(entries) =>
      Json.obj(
        "type" -> clientOutMessage.typeName,
        "message" -> clientOutMessage.message,
        "entries" -> entries
      )
    case SaveResponseMessage(name, message) =>
      Json.obj(
        "type" -> clientOutMessage.typeName,
        "message" -> message
      )
    case _ =>
      Json.obj(
        "type" -> clientOutMessage.typeName,
        "message" -> clientOutMessage.message
      )
  }
}
