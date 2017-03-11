package loamstream.oracle.uger

/**
 * @author clint
 * Mar 7, 2017
 */
sealed abstract class Queue(val name: String) {
  def shorter: Queue
  def longer: Queue
  
  final def isShort: Boolean = this == Queue.Short
  final def isLong: Boolean = this == Queue.Long
}
  
object Queue {
  case object Short extends Queue("short") {
    override def shorter: Queue = this
    override def longer: Queue = Long
  }
  case object Long extends Queue("long") {
    override def shorter: Queue = Short
    override def longer: Queue = this
  }
  
  def fromString(s: String): Option[Queue] = s.toLowerCase.trim match {
    case Short.name => Some(Short)
    case Long.name => Some(Long)
    case _ => None
  }
}
