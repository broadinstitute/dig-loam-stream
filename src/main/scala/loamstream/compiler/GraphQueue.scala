package loamstream.compiler

import scala.collection.mutable.Queue
import loamstream.util.Loggable

/**
 * @author clint
 * Jul 10, 2017
 */
final class GraphQueue extends Loggable {
  private[this] val lock = new AnyRef
  
  private[this] val queue: Queue[GraphThunk] = Queue.empty
  
  private[compiler] def size: Int = queue.size
  
  def enqueue(graphHandle: GraphThunk): this.type = lock.synchronized {
    trace("Enqueueing graph thunk")
    
    queue.enqueue(graphHandle)
    
    this
  }
  
  def dequeue(): GraphThunk = lock.synchronized { 
    val result = queue.dequeue()
    
    trace(s"Dequeued graph thunk; queue ${if(isEmpty) "EMPTY" else "NOT Empty"} afterwards")
    
    result
  }
  
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
