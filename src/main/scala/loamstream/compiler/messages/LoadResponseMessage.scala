package loamstream.compiler.messages

/**
  * LoamStream
  * Created by oliverr on 6/1/2016.
  */
case class LoadResponseMessage(name: String, content: String) extends ClientOutMessage {
  override val typeName: String = "load"

  override def message: String = s"Loaded $name."
}
