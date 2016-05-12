package loamstream.web.controllers.socket

import akka.actor.{Actor, ActorRef, Props}
import loamstream.web.parser.ClientHandler
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object WebSocketReceiveActor {
  def props(sendActor: ActorRef): Props = Props(new WebSocketReceiveActor(sendActor))
}

class WebSocketReceiveActor(sendActor: ActorRef) extends Actor with SocketMessageHandler.OutMessageSink {

  val clientHandler = ClientHandler(this)

  override def receive: Receive = {
    case json: JsValue =>
      json.validate(SocketJsonReader) match {
        case JsSuccess(inMessage, _) =>
          clientHandler.handleInMessage(inMessage)
        case error: JsError =>
          val outJson = Json.obj("type" -> "error", "error" -> JsError.toJson(error))
          sendActor ! outJson
      }
  }

  override def send(outMessage: SocketOutMessage): Unit = {
    sendActor ! SocketJsonWriter.writes(outMessage)
  }
}
