package loamstream.compiler

/**
  * LoamStream
  * Created by oliverr on 6/22/2016.
  */
case class ListResponseMessage(entries: Seq[String]) extends ClientOutMessage {
  override val typeName: String = "list"

  override def message: String = s"Found ${entries.mkString("'", "', '", "'")}"
}
