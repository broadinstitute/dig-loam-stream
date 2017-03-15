package loamstream.uger

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed abstract class Queue(val name: String) {
  final def isShort: Boolean = this == Queue.Short
  final def isLong: Boolean = this == Queue.Long
}

object Queue {
  case object Short extends Queue("Short")
  case object Long extends Queue("Long")

  def fromString(s: String): Queue = s match {
    case Short.name => Short
    case Long.name => Long
    case _ => throw new RuntimeException(s"Queue type $s must be ${Short.name} or ${Long.name}")
  }
}
