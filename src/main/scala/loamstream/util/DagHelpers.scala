package loamstream.util

/**
 * @author clint
 * date: Jun 13, 2016
 */
trait DagHelpers[A] { self: A =>
  def isLeaf: Boolean
  
  def leaves: Set[A]
}