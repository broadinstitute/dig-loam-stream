package loamstream.dag

import loamstream.dag.LoamDag.{Node, StoreNode, ToolNode}
import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
case class LoamDag(loam: LoamGraph) extends Dag {

  type Node = LoamDag.Node

  val storeNodes: Set[StoreNode] = loam.stores.map(StoreNode)
  val toolNodes: Set[ToolNode] = loam.tools.map(ToolNode)

  override def nodes: Set[Node] = storeNodes ++ toolNodes

  override def nextUp(node: Node): Set[Node] = node match {
    case StoreNode(store) => loam.storeProducers.get(store).map(ToolNode).map(Set[Node](_)).getOrElse(Set.empty[Node])
    case ToolNode(tool) => loam.toolInputs.getOrElse(tool, Set.empty).map(StoreNode)
  }

  override def nextDown(node: Node): Set[Node] = node match {
    case StoreNode(store) => loam.storeConsumers.getOrElse(store, Set.empty).map(ToolNode)
    case ToolNode(tool) => loam.toolOutputs.getOrElse(tool, Set.empty).map(StoreNode)
  }
}

object LoamDag {

  trait Node extends Dag.NodeBase

  case class StoreNode(store: Store) extends Node {
    override def label: String = store.toString
  }

  case class ToolNode(tool: Tool) extends Node {
    override def label: String = tool.toString
  }

}
