package loamstream.web.controllers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.google.inject.Inject
import loamstream.web.controllers.socket.WebSocketReceiveActor
import play.api.Environment
import play.api.libs.json.JsValue
import play.api.libs.streams.ActorFlow
import play.api.mvc.{Action, AnyContent, Controller, WebSocket}

/**
  * LoamStream
  * Created by oliverr on 5/3/2016.
  */
class WebController @Inject()(implicit system: ActorSystem, materializer: Materializer,
                              environment: Environment) extends Controller {

  def index: Action[AnyContent] = Action { implicit request =>
    Ok(loamstream.web.views.html.Application.index())
  }

  def socket: WebSocket = WebSocket.accept[JsValue, JsValue] { request =>
    ActorFlow.actorRef(sendActor => WebSocketReceiveActor.props(sendActor, environment))
  }

}
