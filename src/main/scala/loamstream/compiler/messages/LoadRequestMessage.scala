package loamstream.compiler.messages

/** A request by a client to load Loam script form a repository
  *
  * @param name Name of the script to be loaded
  */
final case class LoadRequestMessage(name: String) extends ClientInMessage {

}
