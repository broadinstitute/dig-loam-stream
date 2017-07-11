package loamstream.compiler

import loamstream.loam.LoamGraph

/**
 * @author clint
 * Jul 10, 2017
 */
trait GraphSource {
  def iterator: Iterator[GraphThunk]
}

object GraphSource {
  object Empty extends GraphSource {
    override def iterator: Iterator[GraphThunk] = Iterator.empty
  }
  
  def fromQueue(graphQueue: GraphQueue): GraphSource = new GraphSource {
    override def iterator: Iterator[GraphThunk] = new Iterator[GraphThunk] {
      override def hasNext: Boolean = graphQueue.nonEmpty
      
      override def next(): GraphThunk = graphQueue.dequeue()
    }
  }
}
