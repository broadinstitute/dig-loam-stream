package loamstream.loam

import loamstream.util.ValueBox
import loamstream.compiler.GraphQueue

/**
 * @author clint
 * Jul 10, 2017
 */
final case class LoamProjectState(graph: LoamGraph, graphsSoFar: GraphQueue) {
  def mapGraph(f: LoamGraph => LoamGraph): LoamProjectState = copy(graph = f(graph))
  
  def mapGraphsSoFar(f: GraphQueue => GraphQueue): LoamProjectState = copy(graphsSoFar = f(graphsSoFar))
}

object LoamProjectState {
  def initial: LoamProjectState = LoamProjectState(LoamGraph.empty, GraphQueue.Empty)
}
