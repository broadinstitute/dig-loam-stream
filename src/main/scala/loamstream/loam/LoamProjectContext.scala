package loamstream.loam

import loamstream.conf.LoamConfig
import loamstream.util.ValueBox

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val config: LoamConfig, private val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  def updateGraph(f: LoamGraph => LoamGraph): Unit = graphBox.mutate(f)
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  def empty(config: LoamConfig): LoamProjectContext = {
    new LoamProjectContext(config, ValueBox(LoamGraph.empty))
  }
}
