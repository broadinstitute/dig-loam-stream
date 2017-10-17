package loamstream.dag

import loamstream.dag.WomDag.Node
import wom.graph.{Graph, GraphNode}

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
case class WomDag(wom: Graph) extends Dag {

  type Node = WomDag.Node

  override def nodes: Set[Node] = wom.nodes.map(Node)

  override def nextUp(node: Node): Set[Node] = node.upstream

  private val nodeToNextAfter: Map[Node, Set[Node]] =
    nodes.flatMap(node => node.upstream.map(nodeBefore => (nodeBefore, node))).groupBy(_._1).mapValues(_.map(_._2))
      .view.force

  override def nextDown(node: Node): Set[Node] = nodeToNextAfter.getOrElse(node, Set.empty)
}

object WomDag {

  case class Node(graphNode: GraphNode) extends Dag.NodeBase {
    def upstream: Set[Node] = graphNode.upstream.map(Node)

    override def label: String = graphNode.toString
  }

}
