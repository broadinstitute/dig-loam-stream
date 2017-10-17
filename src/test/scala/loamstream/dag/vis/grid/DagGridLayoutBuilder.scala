package loamstream.dag.vis.grid

import loamstream.dag.Dag
import loamstream.dag.vis.DagLayout

/**
  * LoamStream
  * Created by oliverr on 10/17/2017.
  */
object DagGridLayoutBuilder extends DagLayout.Builder[DagGridLayout] {
  override def build(dag: Dag) = {
    DagGridLayout(dag)
  }
}
