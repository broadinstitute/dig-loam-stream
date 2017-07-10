package loamstream.compiler

import loamstream.loam.LoamGraph
import scala.collection.immutable.Queue
import loamstream.util.ValueBox

/**
 * @author clint
 * Jul 10, 2017
 */
final case class GraphQueue(private val queue: Queue[() => LoamGraph] = Queue.empty) {
  
  def enqueue(graphHandle: () => LoamGraph): GraphQueue = {
    GraphQueue(queue.enqueue(graphHandle))
  }
  
  def dequeue(): (() => LoamGraph, GraphQueue) = { 
    val (popped, newQueue) = queue.dequeue
    
    (popped, GraphQueue(newQueue))
  }
  
  def isEmpty: Boolean = queue.isEmpty
  
  def toSeq: Seq[() => LoamGraph] = queue.toSeq
}

object GraphQueue {
  val Empty: GraphQueue = GraphQueue(Queue.empty)
}
