package loamstream.web.controllers.socket

import loamstream.compiler.messages.{ClientOutMessage, ListResponseMessage, LoadResponseMessage, SaveResponseMessage}
import play.api.libs.json.{JsValue, Json, Writes}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonWriter extends Writes[ClientOutMessage] {
  override def writes(message: ClientOutMessage): JsValue = message match {
    case LoadResponseMessage(name, content) =>
      Json.obj(
        "type" -> message.typeName,
        "message" -> message.message,
        "content" -> content
      )
    case ListResponseMessage(entries) =>
      Json.obj(
        "type" -> message.typeName,
        "message" -> message.message,
        "entries" -> entries
      )
    case SaveResponseMessage(name) =>
      Json.obj(
        "type" -> message.typeName,
        "message" -> message.message
      )
    case _ =>
      Json.obj(
        "type" -> message.typeName,
        "message" -> message.message
      )
  }
}
