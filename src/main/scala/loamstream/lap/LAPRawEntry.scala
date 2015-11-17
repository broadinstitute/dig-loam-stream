package loamstream.lap

/**
 * LoamStream
 * Created by oliverr on 10/9/2015.
 */
case class LAPRawEntry(raw: String) extends LAPEntry {
  override def asString: String = raw
}
