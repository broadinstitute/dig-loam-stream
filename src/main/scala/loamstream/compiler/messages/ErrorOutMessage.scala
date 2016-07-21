package loamstream.compiler.messages

/** A message to the client that an error has occurred */
final case class ErrorOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "error"
}
