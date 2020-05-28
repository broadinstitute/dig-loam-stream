package loamstream.loam

import loamstream.util.DepositBox
import loamstream.util.ValueBox
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import loamstream.model.Tool

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val config: LoamConfig, private val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  def updateGraph(f: LoamGraph => LoamGraph): Unit = graphBox.mutate(f)
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox: DepositBox[LoamProjectContext] = DepositBox.empty

  def empty(config: LoamConfig): LoamProjectContext = {
    new LoamProjectContext(config, ValueBox(LoamGraph.empty))
  }
}
