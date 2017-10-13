package loamstream.dag

/**
  * LoamStream
  * Created by oliverr on 10/10/2017.
  */
trait Dag {
  type Node

  def nodes: Iterable[Node]

  def nextBefore(node: Node): Set[Node]
}

