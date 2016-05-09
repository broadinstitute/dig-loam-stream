package loamstream.web.controllers.socket

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
case class ErrorOutMessage(message: String) extends SocketOutMessage {
  override def typeName: String = "error"
}
