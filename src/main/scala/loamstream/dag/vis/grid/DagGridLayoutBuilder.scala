package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayout.NodeRow

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
case class DagGridLayoutBuilder(nCols: Int) extends DagLayout.Builder[DagGridLayout] {

  override def build(dag: Dag) = {
    val nodeRows: Seq[NodeRow] = Seq.empty
    for (levelNodes <- dag.levelsFromBottom.reverse) {

    }
    DagGridLayout(dag, nodeRows)
  }
}

object DagGridLayoutBuilder {

  case class NodePlacement[D <: Dag](dag: D, nodeRows: Seq[NodeRow], unplacedLevelNodes: Seq[Set[D#Node]]) {
    def currentRow: NodeRow = nodeRows.last

    def previousRowOpt: Option[NodeRow] = {
      val size = nodeRows.size
      if(size < 2) None else Option(nodeRows(size - 2))
    }

    def evaluatePlacement(node: D#Node, iCol: Int): Double = {
      ???
    }
  }

  object NodePlacement {
    def apply[D <: Dag, N <: D#Node](dag:D, nCols: Int): NodePlacement[D] =
      NodePlacement(
        dag = dag,
        nodeRows = Seq(NodeRow.empty(nCols)),
        unplacedLevelNodes = dag.levelsFromBottom.map(_.map(_.asInstanceOf[D#Node])).reverse
      )
  }

}