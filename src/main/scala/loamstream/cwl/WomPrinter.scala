package loamstream.cwl

import wom.graph.Graph

/**
  * LoamStream
  * Created by oliverr on 10/5/2017.
  */
object WomPrinter {

  def print(wom: Graph): String = {
    val nodes = wom.nodes
    val nodeWeights = nodes.map(node => (node, node.upstreamAncestry.size)).toMap
    ???
  }

}
