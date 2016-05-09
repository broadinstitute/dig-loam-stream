package loamstream.web.controllers.socket

import java.util.Date

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object ReceiptOutMessage {
  def apply(text: String): ReceiptOutMessage = ReceiptOutMessage(System.currentTimeMillis(), text.length)
}

case class ReceiptOutMessage(time: Long, size: Int) extends SocketOutMessage {
  override def typeName: String = "receipt"

  def timeAsString: String = new Date(time).toString

  override def message: String = s"At $timeAsString, received text of length $size."
}
