package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.ValueBox

/** Container for compile time and run time context */
final class LoamContext(val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  val fileManager : LoamFileManager = new LoamFileManager
}

object LoamContext {
  def empty: LoamContext = new LoamContext(ValueBox(LoamGraph.empty))
}