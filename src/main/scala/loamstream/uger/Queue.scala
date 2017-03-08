package loamstream.uger

/**
 * @author kyuksel
 *         date: 3/7/17
 */
sealed trait Queue {
  def name: String = toString

  final def isShort: Boolean = this == Queue.Short
  final def isLong: Boolean = this == Queue.Long
}

object Queue {
  case object Short extends Queue
  case object Long extends Queue

  def fromString(name: String): Option[Queue] =
    if (name == Short.toString) { Some(Short) }
    else if (name == Long.toString) { Some(Long) }
    else { None }
}
