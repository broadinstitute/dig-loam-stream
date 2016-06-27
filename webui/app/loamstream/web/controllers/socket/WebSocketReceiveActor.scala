package loamstream.web.controllers.socket

import akka.actor.{Actor, ActorRef, Props}
import loamstream.compiler.{ClientMessageHandler, ClientOutMessage}
import play.api.Environment
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object WebSocketReceiveActor {
  def props(sendActor: ActorRef, environment: Environment): Props =
    Props(new WebSocketReceiveActor(sendActor, environment))
}

class WebSocketReceiveActor(sendActor: ActorRef, environment: Environment)
  extends Actor with ClientMessageHandler.OutMessageSink {

  val clientHandler = ClientMessageHandler(this)

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

  override def send(outMessage: ClientOutMessage): Unit = {
    sendActor ! SocketJsonWriter.writes(outMessage)
  }
}
