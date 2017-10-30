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
  }

  def empty(nCols: Int): NodeRow = NodeRow(Seq.fill(nCols)(None))

}
