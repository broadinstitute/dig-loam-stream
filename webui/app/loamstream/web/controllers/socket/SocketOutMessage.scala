package loamstream.web.controllers.socket

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
trait SocketOutMessage {

  def typeName: String
  def message: String

}
