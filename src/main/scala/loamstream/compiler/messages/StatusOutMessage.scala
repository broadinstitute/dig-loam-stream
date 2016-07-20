package loamstream.compiler.messages

/** A message to client with a status update */
final case class StatusOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "status"
}
