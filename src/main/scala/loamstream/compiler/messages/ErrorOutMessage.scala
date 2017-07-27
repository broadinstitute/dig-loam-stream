package loamstream.compiler.messages

/** A message to the client that an error has occurred */
final case class ErrorOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "error"
}

/** A warning message to the client */
final case class WarningOutMessage(message: String) extends ClientOutMessage {
  override def typeName: String = "warn"
}
