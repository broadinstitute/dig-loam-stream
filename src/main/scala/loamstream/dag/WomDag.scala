package loamstream.dag

import wom.graph.{Graph, GraphNode}

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
case class WomDag(wom: Graph) extends Dag {
  override type Node = GraphNode

  override def nodes: Set[GraphNode] = wom.nodes
}
