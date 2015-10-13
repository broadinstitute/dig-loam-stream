package loamstream.camelbricks

/**
 * LoamStream
 * Created by oliverr on 10/9/2015.
 */
case class CamelBricksRawEntry(raw: String) extends CamelBricksEntry {
  override def asString: String = raw
}
