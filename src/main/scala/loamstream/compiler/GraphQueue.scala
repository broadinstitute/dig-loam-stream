package loamstream.compiler

import scala.collection.mutable.Queue

/**
 * @author clint
 * Jul 10, 2017
 */
final class GraphQueue {
  private[this] val lock = new AnyRef
  
  private[this] val queue: Queue[GraphThunk] = Queue.empty
  
  def enqueue(graphHandle: GraphThunk): this.type = lock.synchronized {
    queue.enqueue(graphHandle)
    
    this
  }
  
  def dequeue(): GraphThunk = lock.synchronized { queue.dequeue() }
  
  def isEmpty: Boolean = lock.synchronized { queue.isEmpty }
  
  def nonEmpty: Boolean = !isEmpty
}

object GraphQueue {
  def empty: GraphQueue = new GraphQueue
  
  def apply(thunks: GraphThunk*): GraphQueue = {
    val q = empty
    
    thunks.foreach(q.enqueue)
    
    q
  }
}
