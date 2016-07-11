package loamstream.compiler.messages

/** A message submitting a Loam script to be compiled and run */
case class RunRequestMessage(code: String) extends ClientInMessage {

}
