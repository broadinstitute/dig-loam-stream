package loamstream.web.controllers.socket

import play.api.libs.json.{JsValue, Json, Writes}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketJsonWriter extends Writes[SocketOutMessage] {
  override def writes(message: SocketOutMessage): JsValue = Json.obj(
    "type" -> message.typeName,
    "message" -> message.message
  )
}
