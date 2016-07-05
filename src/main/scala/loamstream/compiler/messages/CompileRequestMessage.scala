package loamstream.compiler.messages

/** A message submitting a Loam script to be compiled into an execution plan */
case class CompileRequestMessage(code: String) extends ClientInMessage {

}
