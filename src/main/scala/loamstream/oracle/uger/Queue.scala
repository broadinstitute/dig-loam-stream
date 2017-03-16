package loamstream.oracle.uger

/**
 * @author clint
 * Mar 7, 2017
 */
sealed abstract class Queue(val name: String) {
  final def shorter: Queue = if(isShort) this else Queue.Short
  final def longer: Queue = if(isLong) this else Queue.Long
  
  final def isShort: Boolean = this == Queue.Short
  final def isLong: Boolean = this == Queue.Long
}
  
object Queue {
  case object Short extends Queue("short")
  
  case object Long extends Queue("long")
  
  def fromString(s: String): Option[Queue] = s.trim.toLowerCase match {
    case Short.name => Some(Short)
    case Long.name => Some(Long)
    case _ => None
  }
}
