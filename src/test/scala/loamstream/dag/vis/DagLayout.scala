package loamstream.dag.vis

import loamstream.dag.Dag

/**
  * LoamStream
  * Created by oliverr on 10/16/2017.
  */
trait DagLayout {
  def dag: Dag
}

object DagLayout {

  trait Builder[L <: DagLayout] {
    def build(dag: Dag): L
  }

  trait Renderer[L <: DagLayout] {
    def render(layout: L): SVG
  }

}
