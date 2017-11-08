package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayout.NodeRow
import loamstream.dag.vis.grid.DagGridLayoutBuilder.NodesPlacement

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
case class DagGridLayoutBuilder[D <: Dag](nCols: Int) extends DagLayout.Builder[D, DagGridLayout] {

  private def evaluatePlacement(placement: NodesPlacement[D])(node: placement.dag.Node, iCol: Int): Double = {
    ???
  }


  override def build(dag: D): DagGridLayout[D] = {
    val nodeRows: Seq[NodeRow[D]] = Seq.empty
    for (levelNodes <- dag.levelsFromBottom.reverse) {

    }
    DagGridLayout(dag, nodeRows)
  }
}

object DagGridLayoutBuilder {

  case class NodesPlacement[D <: Dag](dag: D, nodeRows: Seq[NodeRow[D]], unplacedLevelNodes0: Seq[Set[D#Node]]) {

    def unplacedLevelNodes: Seq[Set[dag.Node]] = unplacedLevelNodes0.map(_.map(_.asInstanceOf[dag.Node]))

    def currentRow: NodeRow[D] = nodeRows.last

    def previousRowOpt: Option[NodeRow[D]] = {
      val size = nodeRows.size
      if (size < 2) None else Option(nodeRows(size - 2))
    }

    case class NextNodePlacement(node: dag.Node, iCol: Int)

    def findAllNextNodePlacements: Set[NextNodePlacement] = ???

    def +(nextNodePlacement: NextNodePlacement): NodesPlacement[D] = ???

  }

  object NodesPlacement {
    def apply[D <: Dag, N <: D#Node](dag: D, nCols: Int): NodesPlacement[D] =
      NodesPlacement(
        dag = dag,
        nodeRows = Seq(NodeRow.empty[D](dag, nCols)),
        unplacedLevelNodes0 = dag.levelsFromBottom.map(_.map(_.asInstanceOf[D#Node])).reverse
      )
  }

}