package loamstream.dag.vis

import loamstream.dag.Dag
import loamstream.svg.SVG
import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 10/16/2017.
  */
trait DagLayout[D <: Dag] {
  def dag: D
}

object DagLayout {

  trait Builder[D <: Dag, L[DD <: Dag] <: DagLayout[DD]] {
    def build(dag: D): L[D]
  }

  trait Renderer[D <: Dag, L[DD <: Dag] <: DagLayout[DD]] {
    def render(layout: L[D]): SVG
  }

}
