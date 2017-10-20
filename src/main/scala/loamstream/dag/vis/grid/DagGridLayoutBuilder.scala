package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout
import loamstream.dag.vis.grid.DagGridLayout.NodeRow

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
class DagGridLayoutBuilder extends DagLayout.Builder[DagGridLayout] {
  override def build(dag: Dag) = {
    val nodeRows : Seq[NodeRow] = Seq.empty

    DagGridLayout(dag, nodeRows)
  }
}
