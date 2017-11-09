package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayoutBuilder.DagBox

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
case class DagGridLayoutBuilder[D <: Dag](nCols: Int) extends DagLayout.Builder[D, DagGridLayout] {

  private def placementCost(dagBox: DagBox[D])(nodesPlacement: dagBox.NodesPlacement)(
    nextNodePlacement: nodesPlacement.NextNodePlacement): Double = {
    val nodeToPlace = nextNodePlacement.node
    val pos = nextNodePlacement.pos
    dagBox.dag.nextUp(nodeToPlace).flatMap(nodesPlacement.nodePoses.get(_)).map(pos.distanceTo(_)).sum
  }


  override def build(dag: D): DagGridLayout[D] = {
    val dagBox = DagBox(dag, nCols)
    var nodesPlacement = dagBox.NodesPlacement.empty
    while (!nodesPlacement.isComplete) {
      val nodesPlacementOld = nodesPlacement
      val nextNodePlacementCost: nodesPlacementOld.NextNodePlacement => Double =
        (nextNodePlacement: nodesPlacementOld.NextNodePlacement) =>
          placementCost(dagBox)(nodesPlacementOld)(nextNodePlacement)
      val nextNodePlacement = nodesPlacementOld.findAllNextNodePlacements.minBy(nextNodePlacementCost)
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
        DagGridLayout.NodeRow(dag, nCols, nodeOpts)

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

    import NodesPlacement.NodePos

    case class NodesPlacement(nodeRows: Seq[NodeRowBuilder], nodePoses: Map[dag.Node, NodePos],
                              unplacedLevelNodes: Seq[Set[dag.Node]]) {

      def currentRow: NodeRowBuilder = nodeRows.last

      def previousRowOpt: Option[NodeRowBuilder] = {
        val size = nodeRows.size
        if (size < 2) None else Option(nodeRows(size - 2))
      }

      case class NextNodePlacement(node: dag.Node, pos: NodePos)

      def findAllNextNodePlacements: Set[this.NextNodePlacement] = for (
        node <- unplacedLevelNodes.head;
        iCol <- currentRow.emptyICols
      ) yield NextNodePlacement(node, NodePos(nodeRows.size, iCol))

      def +(nextNodePlacement: this.NextNodePlacement): NodesPlacement = {
        val placedNode = nextNodePlacement.node
        val pos = nextNodePlacement.pos
        val iColPlaced = pos.iCol
        val newCurrentRow = currentRow.add(placedNode, iColPlaced)
        val nodeRowsWithNewNode = nodeRows.updated(nodeRows.size - 1, newCurrentRow)
        val nodeRowsNew = if (nodeRowsWithNewNode.last.isFull) {
          nodeRowsWithNewNode :+ NodeRowBuilder.empty
        } else {
          nodeRowsWithNewNode
        }
        val nodePosesNew = NodesPlacement.this.nodePoses + (placedNode -> pos)
        val unplacedLevelNodesMinusPlacedNode = unplacedLevelNodes.updated(0, unplacedLevelNodes.head - placedNode)
        val unplacedLevelNodesNew = if (unplacedLevelNodesMinusPlacedNode.head.isEmpty) {
          unplacedLevelNodesMinusPlacedNode.tail
        } else {
          unplacedLevelNodesMinusPlacedNode
        }
        NodesPlacement(nodeRowsNew, nodePosesNew, unplacedLevelNodesNew)
      }

      def isComplete: Boolean = unplacedLevelNodes.isEmpty

    }

    object NodesPlacement {
      def empty: NodesPlacement =
        NodesPlacement(
          nodeRows = Seq(NodeRowBuilder.empty),
          nodePoses = Map.empty,
          unplacedLevelNodes = dag.levelsFromBottom.reverse
        )

      case class NodePos(iRow: Int, iCol: Int) {
        def distanceTo(oPos: NodePos): Int = Math.abs(oPos.iRow - iRow) + Math.abs(oPos.iCol - iCol)
      }

    }

  }

}