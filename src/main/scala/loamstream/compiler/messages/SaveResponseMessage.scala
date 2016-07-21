package loamstream.compiler.messages

/** A message sent to client that a Loam script has been successfully saved
  *
  * @param name    Name of Loam script
  * @param message Message about the saving such as where it was saved to
  */
final case class SaveResponseMessage(name: String, message: String) extends ClientOutMessage {
  override val typeName: String = "save"
}
