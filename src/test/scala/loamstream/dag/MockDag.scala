package loamstream.dag

import loamstream.dag.MockDag.Node

/**
  * LoamStream
  * Created by oliverr on 11/7/2017.
  */
case class MockDag(idToNode: Map[String, Node], nodeToNextUp: Map[Node, Set[Node]],
                   nodeToNextDown: Map[Node, Set[Node]]) extends Dag {
  override type Node = MockDag.Node

  override def nodes: Set[Node] = idToNode.values.toSet

  override def nextUp(node: Node): Set[Node] = nodeToNextUp.getOrElse(node, Set.empty)

  override def nextDown(node: Node): Set[Node] = nodeToNextDown.getOrElse(node, Set.empty)
}

object MockDag {

  def empty: MockDag = MockDag(Map.empty, Map.empty, Map.empty)

  case class Node(id: String) extends Dag.NodeBase {
    override def label: String = id
  }

}
