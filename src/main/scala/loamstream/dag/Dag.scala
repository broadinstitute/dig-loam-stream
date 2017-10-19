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

  def groupTraverse[R](startGroup: Set[Node], start: R, next: (Set[Node], Set[Node]) => Set[Node],
                       merge: (R, Set[Node]) => R, keepGoingFor: Set[Node] => Boolean): R = {
    var group: Set[Node] = startGroup
    var previousNodes: Set[Node] = Set.empty
    var result: R = start
    while (keepGoingFor(group)) {
      result = merge(result, group)
      previousNodes ++= group
      group = next(group, previousNodes)
    }
    result
  }

  def nodesAbove(node: Node): Set[Node] =
    groupTraverse[Set[Node]](
      startGroup = nextUp(node),
      start = Set.empty,
      next = _.flatMap(nextUp) -- _,
      merge = _ ++ _,
      keepGoingFor = _.nonEmpty
    )

  def nodesBelow(node: Node): Set[Node] =
    groupTraverse[Set[Node]](
      startGroup = nextDown(node),
      start = Set.empty,
      next = _.flatMap(nextDown) -- _,
      merge = _ ++ _,
      keepGoingFor = _.nonEmpty
    )

  def levelsFromTop: Seq[Set[Node]] =
    groupTraverse(
      startGroup = topNodes,
      start = Seq.empty[Set[Node]],
      next =
        (levelNodes, previousNodes) =>
          levelNodes.flatMap(nextDown).filter(node => nextUp(node).forall(previousNodes)),
      merge = (levels: Seq[Set[Node]], nodes: Set[Node]) => levels :+ nodes,
      keepGoingFor = _.nonEmpty
    )

  def levelsFromBottom: Seq[Set[Node]] =
    groupTraverse(
      startGroup = bottomNodes,
      start = Seq.empty[Set[Node]],
      next =
        (levelNodes, previousNodes) =>
          levelNodes.flatMap(nextUp).filter(node => nextDown(node).forall(previousNodes)),
      merge = (levels: Seq[Set[Node]], nodes: Set[Node]) => levels :+ nodes,
      keepGoingFor = _.nonEmpty
    )

  def ungroupLevels(groups: Seq[Set[Node]]): Map[Node, Int] = groups.zipWithIndex.flatMap {
    case (nodes: Set[Node], levelIndex: Int) => nodes.map(node => (node, levelIndex))
  }.toMap

  def nodesToLevelsFromTop: Map[Node, Int] = ungroupLevels(levelsFromTop)

  def nodesToLevelsFromBottom: Map[Node, Int] = ungroupLevels(levelsFromBottom)

}

object Dag {
  trait NodeBase {
    def label: String
  }
}

