package loamstream.compiler.messages

/** Response to a load request, containing the Loam script to be loaded.
  *
  * @param name Name of Loam script
  * @param content Content of Loam script
  * @param message A human-readable message typically describing where script was loaded from
  */
case class LoadResponseMessage(name: String, content: String, message: String) extends ClientOutMessage {
  override val typeName: String = "load"
}
