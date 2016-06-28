package loamstream.compiler.messages

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
case class LoadResponseMessage(name: String, content: String, message: String) extends ClientOutMessage {
  override val typeName: String = "load"
}
