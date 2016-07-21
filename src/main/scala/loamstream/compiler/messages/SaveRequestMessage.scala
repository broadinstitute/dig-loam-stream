package loamstream.compiler.messages

/** A message from client requesting to save a Loam script
  *
  * @param name    Name of Laom script
  * @param content Contant of Loam script
  */
final case class SaveRequestMessage(name: String, content: String) extends ClientInMessage {

}
