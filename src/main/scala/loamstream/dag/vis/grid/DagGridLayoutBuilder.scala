package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayoutBuilder.DagBox

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
case class DagGridLayoutBuilder[D <: Dag](nCols: Int) extends DagLayout.Builder[D, DagGridLayout] {

  private def evaluatePlacement(dagBox: DagBox[D])(placement: dagBox.NodesPlacement)(
    node: dagBox.dag.Node, iCol: Int): Double = {
    ???
  }


  override def build(dag: D): DagGridLayout[D] = {
    val dagBox = DagBox(dag, nCols)
    val nodeRows: Seq[dagBox.NodeRowBuilder] = Seq.empty
    for (levelNodes <- dag.levelsFromBottom.reverse) {

    }
    DagGridLayout(dag, nodeRows.map(_.toNodeRow))
  }
}

object DagGridLayoutBuilder {

  case class DagBox[D <: Dag](dag: D, nCols: Int) {

    case class NodeRowBuilder(nodeOpts: Seq[Option[dag.Node]]) extends DagGridLayout.NodeRowBase[D] {

      override def dag: DagBox.this.dag.type = DagBox.this.dag

      override def nCols: Int = DagBox.this.nCols

      override def toNodeRow: DagGridLayout.NodeRow[D] =
        DagGridLayout.NodeRow(dag, nCols, nodeOpts.map(_.get))

      def add(node: dag.Node, iCol: Int): NodeRowBuilder = {
        val nodeOptsNew: Seq[Option[dag.Node]] = nodeOpts.updated(iCol, Option(node))
        NodeRowBuilder(nodeOptsNew)
      }

      def isEmpty: Boolean = nodeOpts.forall(_.isEmpty)

      def isFull: Boolean = nodeOpts.forall(_.nonEmpty)

      def emptyICols: Seq[Int] = (0 until nCols).filter(nodeOpts(_).isEmpty)

      def iColsFromCenter: Seq[Int] = (0 until nCols).map(i => (nCols / 2) + (1 - 2 * (i % 2)) * ((i + 1) / 2))

      def emptyIColsFromCenter: Seq[Int] = iColsFromCenter.filter(nodeOpts(_).isEmpty)

    }

    object NodeRowBuilder {
      def empty: NodeRowBuilder = NodeRowBuilder(Seq.fill(nCols)(None))
    }

    case class NodesPlacement(nodeRows: Seq[NodeRowBuilder], unplacedLevelNodes: Seq[Set[dag.Node]]) {

      def currentRow: NodeRowBuilder = nodeRows.last

      def previousRowOpt: Option[NodeRowBuilder] = {
        val size = nodeRows.size
        if (size < 2) None else Option(nodeRows(size - 2))
      }

      case class NextNodePlacement(node: dag.Node, iCol: Int)

      def findAllNextNodePlacements: Set[NextNodePlacement] = for (
        node <- unplacedLevelNodes.head;
        iCol <- currentRow.emptyIColsFromCenter
      ) yield NextNodePlacement(node, iCol)

      def +(nextNodePlacement: NextNodePlacement): NodesPlacement = {
        val placedNode = nextNodePlacement.node
        val iColPlaced = nextNodePlacement.iCol
        val newCurrentRow = currentRow.add(placedNode, iColPlaced)
        val nodeRowsWithNewNode = nodeRows.updated(nodeRows.size - 1, newCurrentRow)
        val nodeRowsNew = if (nodeRowsWithNewNode.last.isFull) {
          nodeRowsWithNewNode :+ NodeRowBuilder.empty
        } else {
          nodeRowsWithNewNode
        }
        val unplacedLevelNodesMinusPlacedNode = unplacedLevelNodes.updated(0, unplacedLevelNodes.head - placedNode)
        val unplacedLevelNodesNew = if (unplacedLevelNodesMinusPlacedNode.head.isEmpty) {
          unplacedLevelNodesMinusPlacedNode.tail
        } else {
          unplacedLevelNodesMinusPlacedNode
        }
        NodesPlacement(nodeRowsNew, unplacedLevelNodesNew)
      }

    }

    object NodesPlacement {
      def empty(nCols: Int): NodesPlacement =
        NodesPlacement(
          nodeRows = Seq(NodeRowBuilder.empty),
          unplacedLevelNodes = dag.levelsFromBottom.reverse
        )
    }

  }

}