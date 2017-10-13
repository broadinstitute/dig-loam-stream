package loamstream.dag

import loamstream.loam.LoamGraph
import loamstream.model.{Store, Tool}

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
case class LoamDag(loam: LoamGraph) extends Dag {

  trait Node

  case class StoreNode(store: Store.Untyped) extends Node

  case class ToolNode(tool: Tool) extends Node

  val storeNodes: Set[StoreNode] = loam.stores.map(StoreNode)
  val toolNodes: Set[ToolNode] = loam.tools.map(ToolNode)

  override def nodes: Set[Node] = storeNodes ++ toolNodes

  override def nextBefore(node: Node): Set[Node] = node match {
    case StoreNode(store) => loam.storeProducers.get(store).map(ToolNode).map(Set[Node](_)).getOrElse(Set.empty[Node])
    case ToolNode(tool) => loam.toolInputs.getOrElse(tool, Set.empty).map(StoreNode)
  }

  override def nextAfter(node: Node): Set[Node] = node match {
    case StoreNode(store) => loam.storeConsumers.getOrElse(store, Set.empty).map(ToolNode)
    case ToolNode(tool) => loam.toolOutputs.getOrElse(tool, Set.empty).map(StoreNode)
  }
}
