package loamstream.compiler

import loamstream.loam.LoamGraph

/**
 * @author clint
 * Jul 10, 2017
 */
trait GraphSource extends Iterable[() => LoamGraph] {
  def get(): () => LoamGraph
  
  override def iterator: Iterator[() => LoamGraph] 
}

object GraphSource {
  lazy val Empty: GraphSource = fromQueue(GraphQueue.empty)
  
  def fromQueue(graphQueue: GraphQueue): GraphSource = new GraphSource {
    override def get(): () => LoamGraph = graphQueue.dequeue()
  
    override def iterator: Iterator[() => LoamGraph] = graphQueue.freeze.iterator
  }
}
