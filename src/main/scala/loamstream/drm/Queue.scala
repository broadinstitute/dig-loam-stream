package loamstream.drm

/**
 * @author clint
 * @author kyuksel
 * Mar 7, 2017
 * 
 * NB: The only queue that now exists on Uger is "broad", but others exist on EBI's LSF.
 *   -Clint May 10, 2018
 */
final case class Queue(val name: String) {
  require(name.nonEmpty)
  
  override def toString: String = name
}
