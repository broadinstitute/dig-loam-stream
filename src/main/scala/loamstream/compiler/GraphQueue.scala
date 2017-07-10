package loamstream.compiler

import loamstream.loam.LoamGraph
import scala.collection.mutable.Queue
import loamstream.util.ValueBox

/**
 * @author clint
 * Jul 10, 2017
 */
final class GraphQueue {
  private[this] val lock = new AnyRef
  
  private[this] val queue: Queue[() => LoamGraph] = Queue.empty
  
  def enqueue(graphHandle: () => LoamGraph): this.type = lock.synchronized {
    queue.enqueue(graphHandle)
    
    this
  }
  
  def dequeue(): () => LoamGraph = lock.synchronized { queue.dequeue() }
  
  def freeze: Iterable[() => LoamGraph] = lock.synchronized { queue.toVector }
  
  def isEmpty: Boolean = lock.synchronized { queue.isEmpty }
  
  def nonEmpty: Boolean = !isEmpty
  
  def orElseJust(g: LoamGraph): GraphQueue = lock.synchronized {
    if(isEmpty) GraphQueue(() => g) else this
  }
}

object GraphQueue {
  def empty: GraphQueue = new GraphQueue
  
  def apply(thunks: (() => LoamGraph)*): GraphQueue = {
    val q = empty
    
    thunks.foreach(q.enqueue)
    
    q
  }
}
