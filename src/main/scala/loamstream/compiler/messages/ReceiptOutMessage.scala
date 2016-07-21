package loamstream.compiler.messages

import java.util.Date

/** A message that a submitted Loam script has been received */
object ReceiptOutMessage {
  def apply(text: String): ReceiptOutMessage = ReceiptOutMessage(System.currentTimeMillis(), text.length)
}

/** A message that a submitted Loam script has been received
  *
  * @param time Time when Loam script was received as millis since epoch
  * @param size String length of Loam script
  */
final case class ReceiptOutMessage(time: Long, size: Int) extends ClientOutMessage {
  override def typeName: String = "receipt"

  def timeAsString: String = new Date(time).toString

  override def message: String = s"At $timeAsString, received text of length $size."
}
