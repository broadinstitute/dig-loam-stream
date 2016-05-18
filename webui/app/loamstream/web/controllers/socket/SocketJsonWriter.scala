package loamstream.web.controllers.socket

import loamstream.compiler.ClientOutMessage
import play.api.libs.json.{JsValue, Json, Writes}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonWriter extends Writes[ClientOutMessage] {
  override def writes(message: ClientOutMessage): JsValue = Json.obj(
    "type" -> message.typeName,
    "message" -> message.message
  )
}
