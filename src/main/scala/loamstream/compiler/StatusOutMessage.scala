package loamstream.compiler

/**
  * LoamStream
  * Created by oliverr on 5/12/2016.
  */
case class StatusOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "status"
}
