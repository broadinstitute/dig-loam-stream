package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.ValueBox

/** Container for compile time and run time context */
class LoamContext {

  val graphBox: ValueBox[LoamGraph] = new ValueBox(LoamGraph.empty)

  def graph: LoamGraph = graphBox.value

  val fileManager : LoamFileManager = new LoamFileManager
}
