package loamstream.compiler.messages

/** A message containing the requested list of names of available Loam scripts */
final case class ListResponseMessage(entries: Seq[String]) extends ClientOutMessage {
  override val typeName: String = "list"

  override def message: String = s"Found ${entries.mkString("'", "', '", "'")}"
}
