package loamstream.compiler.messages

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
case class SaveResponseMessage(name: String, message: String) extends ClientOutMessage {
  override val typeName: String = "save"
}
