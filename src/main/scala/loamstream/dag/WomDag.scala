package loamstream.dag

import wom.graph.{Graph, GraphNode}

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
case class WomDag(wom: Graph) extends Dag {
  override type Node = GraphNode

  override def nodes: Set[GraphNode] = wom.nodes

  override def nextUp(node: GraphNode): Set[Node] = node.upstream

  private val nodeToNextAfter: Map[Node, Set[Node]] =
    nodes.flatMap(node => node.upstream.map(nodeBefore => (nodeBefore, node))).groupBy(_._1).mapValues(_.map(_._2))
      .view.force

  override def nextDown(node: GraphNode): Set[Node] = nodeToNextAfter.getOrElse(node, Set.empty)
}
