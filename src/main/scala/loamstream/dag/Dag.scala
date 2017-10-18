package loamstream.dag

import loamstream.dag.Dag.NodeBase

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
trait Dag {

  type Node <: NodeBase

  def nodes: Set[Node]

  def nextUp(node: Node): Set[Node]

  def nextDown(node: Node): Set[Node]

  def topNodes: Set[Node] = nodes.filter(nextUp(_).isEmpty)

  def bottomNodes: Set[Node] = nodes.filter(nextDown(_).isEmpty)

  def groupTraverse[R](startGroup: Set[Node], start: R, next: Set[Node] => Set[Node], merge: (R, Set[Node]) => R,
                       keepGoingFor: Set[Node] => Boolean): R = {
    var group: Set[Node] = startGroup
    var previousNodes: Set[Node] = Set.empty
    var result: R = start
    while (keepGoingFor(group)) {
      result = merge(result, group)
      previousNodes ++= group
      group = next(group) -- previousNodes
    }
    result
  }

  def nodesAbove(node: Node): Set[Node] =
    groupTraverse[Set[Node]](
      startGroup = nextUp(node),
      start = Set.empty,
      next = _.flatMap(nextUp),
      merge = _ ++ _,
      keepGoingFor = _.nonEmpty
    )

  def nodesBelow(node: Node): Set[Node] =
    groupTraverse[Set[Node]](
      startGroup = nextDown(node),
      start = Set.empty,
      next = _.flatMap(nextDown),
      merge = _ ++ _,
      keepGoingFor = _.nonEmpty
    )

  def levelsFromTop: Map[Int, Set[Node]] =
    groupTraverse(
      startGroup = topNodes,
      start = Map.empty[Int, Set[Node]],
      next = _.flatMap(nodesBelow),
      merge = (levels: Map[Int, Set[Node]], nodes: Set[Node]) => levels + (levels.size -> nodes),
      keepGoingFor = _.nonEmpty
    )

  def levelsFromBottom: Map[Int, Set[Node]] =
    groupTraverse(
      startGroup = bottomNodes,
      start = Map.empty[Int, Set[Node]],
      next = _.flatMap(nodesAbove),
      merge = (levels: Map[Int, Set[Node]], nodes: Set[Node]) => levels + (levels.size -> nodes),
      keepGoingFor = _.nonEmpty
    )

  def unpackLevels(groups: Map[Int, Set[Node]]): Map[Node, Int] = groups.to[Set].flatMap {
    case (key: Int, nodes: Set[Node]) => nodes.map(node => (node, key))
  }.toMap

  def nodesToLevelsFromTop: Map[Node, Int] = unpackLevels(levelsFromTop)

  def nodesToLevelsFromBottom: Map[Node, Int] = unpackLevels(levelsFromBottom)

}

object Dag {
  trait NodeBase {
    def label: String
  }
}

