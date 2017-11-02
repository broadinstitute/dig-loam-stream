package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayout.NodeRow

/**
  * LoamStream
  * Created by oliverr on 10/16/2017.
  */
case class DagGridLayout(dag: Dag, rows: Seq[NodeRow]) extends DagLayout {

}

object DagGridLayout {

  case class NodeRow(nodes: Seq[Option[Dag.NodeBase]]) {
    def add(node: Dag.NodeBase, iCol: Int): NodeRow = NodeRow(nodes.updated(iCol, Option(node)))

    val nCols: Int = nodes.size

    def isEmpty: Boolean = nodes.forall(_.isEmpty)

    def isFull: Boolean = nodes.forall(_.nonEmpty)

    def emptyICols: Seq[Int] = (0 until nCols).filter(nodes(_).isEmpty)

    def iColsFromCenter: Seq[Int] = (0 until nCols).map(i => (nCols / 2) + (1 - 2 * (i % 2)) * ((i + 1) / 2))

    def emptyIColsFromCenter: Seq[Int] = iColsFromCenter.filter(nodes(_).isEmpty)
  }

  object NodeRow {
    def empty(nCols: Int): NodeRow = NodeRow(Seq.fill(nCols)(None))
  }

}
