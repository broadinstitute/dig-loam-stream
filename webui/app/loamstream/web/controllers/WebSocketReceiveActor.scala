package loamstream.web.controllers

import akka.actor.{Actor, ActorRef, Props}
import play.api.libs.json.{JsValue, Json}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object WebSocketReceiveActor {
  def props(sendActor: ActorRef): Props = Props(new WebSocketReceiveActor(sendActor))
}

class WebSocketReceiveActor(sendActor: ActorRef) extends Actor {
  override def receive: Receive = {
    case json: JsValue => println(Json.prettyPrint(json))
  }
}
