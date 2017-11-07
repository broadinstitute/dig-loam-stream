package loamstream.dag

/**
  * LoamStream
  * Created by oliverr on 11/7/2017.
  */
case class MockDag(n: Int) extends Dag {
  override type Node = MockDag.Node

  override def nodes: Set[Node] = ???

  override def nextUp(node: Node): Set[Node] = ???

  override def nextDown(node: Node): Set[Node] = ???
}

object MockDag {

  def empty: MockDag = MockDag(0)

  case class Node(id: String) extends Dag.NodeBase {
    override def label: String = id
  }

}
