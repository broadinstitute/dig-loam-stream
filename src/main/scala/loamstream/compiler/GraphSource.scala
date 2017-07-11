package loamstream.compiler

import loamstream.loam.LoamGraph

/**
 * @author clint
 * Jul 10, 2017
 */
trait GraphSource extends Iterable[GraphThunk]

object GraphSource {
  lazy val Empty: GraphSource = fromQueue(GraphQueue.empty)
  
  def fromQueue(graphQueue: GraphQueue): GraphSource = new GraphSource {
    override def iterator: Iterator[GraphThunk] = new Iterator[GraphThunk] {
      override def hasNext: Boolean = graphQueue.nonEmpty
      
      override def next(): GraphThunk = graphQueue.dequeue()
    }
  }
}
