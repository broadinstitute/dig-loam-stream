package loamstream.compiler.messages

/** A message submitting a Loam script to be compiled and run */
final case class RunRequestMessage(code: String) extends ClientInMessage {

}
