package loamstream.compiler

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
case class ErrorOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "error"
}
