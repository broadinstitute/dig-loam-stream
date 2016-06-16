package loamstream.util

/**
 * @author clint
 * date: Jun 13, 2016
 */
trait DagHelpers[A] { self: A =>
  def isLeaf: Boolean
  
  def leaves: Set[A]
  
  def removeAll(toRemove: Iterable[A]): DagHelpers[A]
  
  def chunks: Stream[Set[A]] = {
    val myLeaves = this.leaves
    
    myLeaves #:: {
      if(isLeaf) { Stream.empty } 
      else { this.removeAll(myLeaves).chunks }
    }
  }
}