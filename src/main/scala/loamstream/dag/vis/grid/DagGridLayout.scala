package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayout.NodeRow

/**
  * LoamStream
  * Created by oliverr on 10/16/2017.
  */
case class DagGridLayout[D <: Dag](dag: D, rows: Seq[NodeRow[D]]) extends DagLayout[D] {

}

object DagGridLayout {

  trait NodeRowBase[D <: Dag] {
    def dag: D

    def nCols: Int

    def nodeOpts: Seq[Option[Dag.NodeBase]]

    def toNodeRow: NodeRow[D]
  }

  case class NodeRow[D <: Dag](dag: D, nCols: Int, nodeOpts: Seq[Option[Dag.NodeBase]]) extends NodeRowBase[D] {
    override def toNodeRow: NodeRow[D] = this
  }

}
