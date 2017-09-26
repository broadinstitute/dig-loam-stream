package loamstream.cwl

import loamstream.util.lines.Lines
import wdl4s.wom.graph.{Graph, GraphNode}

/**
  * LoamStream
  * Created by oliverr on 9/26/2017.
  */
object WomDebug {

  implicit val graphNodePrinter: Lines.Printer[GraphNode] =
    (node: GraphNode) => Lines(node.toString)

  implicit val graphPrinter: Lines.Printer[Graph] =
    (graph: Graph) => Lines(graph.toString)

}
