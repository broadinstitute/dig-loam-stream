package loamstream.web.controllers.socket

/**
  * LoamStream
  * Created by oliverr on 5/12/2016.
  */
case class StatusOutMessage(message: String) extends SocketOutMessage {
  override def typeName: String = "status"
}
