package loamstream.compiler.messages

/** A message sent to a client such as a UI */
trait ClientOutMessage {

  /** The type of message */
  def typeName: String
  /** A human-readable description of the message */
  def message: String

}
