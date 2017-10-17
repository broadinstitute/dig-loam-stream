package loamstream.dag

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
trait Dag {
  type Node

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
      next = _.flatMap(nextUp(_)),
      merge = _ ++ _,
      keepGoingFor = _.nonEmpty
    )

  def nodesBelow(node: Node): Set[Node] =
    groupTraverse[Set[Node]](
      startGroup = nextUp(node),
      start = Set.empty,
      next = _.flatMap(nextUp),
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

  def unpackNodeGroups[K](groups: Map[K, Set[Node]]): Map[Node, K] = groups.toSet.flatMap {
    case (key, nodes) => nodes.map(node => (node, key))
  }.toMap

  def nodesToLevelsFromTop: Map[Node, Int] = unpackNodeGroups(levelsFromTop)

  def nodesToLevelsFromBottom: Map[Node, Int] = unpackNodeGroups(levelsFromBottom)

}

