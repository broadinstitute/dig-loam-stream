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
  
  def isEmpty: Boolean = lock.synchronized { queue.isEmpty }
}
