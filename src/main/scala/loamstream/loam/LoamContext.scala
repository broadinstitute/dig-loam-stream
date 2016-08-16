package loamstream.loam

import loamstream.util.ValueBox

/** Container for compile time and run time context */
class LoamContext {

  val graphBox: ValueBox[LoamGraph] = new ValueBox(LoamGraph.empty)

  def graph: LoamGraph = graphBox.value
}
