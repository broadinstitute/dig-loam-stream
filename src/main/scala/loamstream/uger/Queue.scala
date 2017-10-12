package loamstream.uger

/**
 * @author clint
 * @author kyuksel
 * Mar 7, 2017
 * 
 * NB: The only queue that now exists is "broad", but this seems a prudent abstraction to keep, at least for now.
 *   -Clint Oct 11, 2017
 */
sealed abstract class Queue(val name: String) {
  final def isBroad: Boolean = this == Queue.Broad
}
  
object Queue {
  val Default: Queue = Broad 
  
  case object Broad extends Queue("broad")
  
  def fromString(s: String): Option[Queue] = s.trim.toLowerCase match {
    case Broad.name => Some(Broad)
    case _ => None
  }
}
