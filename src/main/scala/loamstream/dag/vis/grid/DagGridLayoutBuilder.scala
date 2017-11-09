package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayoutBuilder.DagBox

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
case class DagGridLayoutBuilder[D <: Dag](nCols: Int) extends DagLayout.Builder[D, DagGridLayout] {

  private def evaluatePlacement(dagBox: DagBox[D])(nodesPlacement: dagBox.NodesPlacement)(
    nextNodePlacement: nodesPlacement.NextNodePlacement): Double = {
    val nodeToPlace = nextNodePlacement.node
    val iCol = nextNodePlacement.iCol
    val nodeBeforeOpt = nodesPlacement.previousRowOpt.flatMap(_.nodeOpts(iCol))
    val connectToBeforeBonus = nodeBeforeOpt.map { nodeBefore =>
      if (dagBox.dag.nextUp(nodeToPlace).contains(nodeBefore)) 1.0 else 0.0
    }.getOrElse(0.0)
    val nearCenterBonus = 1.0 / (1.0 + Math.abs(iCol - (nCols - 1) / 2.0))
    connectToBeforeBonus + nearCenterBonus
  }


  override def build(dag: D): DagGridLayout[D] = {
    val dagBox = DagBox(dag, nCols)
    var nodesPlacement = dagBox.NodesPlacement.empty
    while (!nodesPlacement.isComplete) {
      val nodesPlacementOld = nodesPlacement
      val nextNodePlacementEval: nodesPlacementOld.NextNodePlacement => Double =
        (nextNodePlacement: nodesPlacementOld.NextNodePlacement) =>
          evaluatePlacement(dagBox)(nodesPlacementOld)(nextNodePlacement)
      val nextNodePlacement = nodesPlacementOld.findAllNextNodePlacements.maxBy(nextNodePlacementEval)
      nodesPlacement = nodesPlacementOld + nextNodePlacement
    }
    val nodeRows = nodesPlacement.nodeRows.map(_.toNodeRow)
    DagGridLayout(dag, nodeRows)
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

      def findAllNextNodePlacements: Set[this.NextNodePlacement] = for (
        node <- unplacedLevelNodes.head;
        iCol <- currentRow.emptyIColsFromCenter
      ) yield NextNodePlacement(node, iCol)

      def +(nextNodePlacement: this.NextNodePlacement): NodesPlacement = {
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

      def isComplete: Boolean = unplacedLevelNodes.isEmpty

    }

    object NodesPlacement {
      def empty: NodesPlacement =
        NodesPlacement(
          nodeRows = Seq(NodeRowBuilder.empty),
          unplacedLevelNodes = dag.levelsFromBottom.reverse
        )
    }

  }

}