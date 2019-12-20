package loamstream.loam

import loamstream.util.DepositBox
import loamstream.util.ValueBox
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import loamstream.model.Tool
import loamstream.util.HeterogeneousMap

/** Container for compile time and run time context for a project */
final class LoamProjectContext private (val config: LoamConfig) {

  private val graphBox: ValueBox[LoamGraph] = ValueBox(LoamGraph.empty)
  
  def graph: LoamGraph = graphBox.value

  def updateGraph(f: LoamGraph => LoamGraph): Unit = graphBox.mutate(f)
  
  private val propertiesMapBox: ValueBox[HeterogeneousMap] = ValueBox(HeterogeneousMap.empty)
  
  def propertiesMap: HeterogeneousMap = propertiesMapBox.value
  
  def addToPropertiesMap(entries: HeterogeneousMap.Entry[_, _]*): Unit = {
    if(entries.nonEmpty) {
      propertiesMapBox.mutate(_ ++ entries)
    }
  }
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox: DepositBox[LoamProjectContext] = DepositBox.empty

  def empty(config: LoamConfig): LoamProjectContext = new LoamProjectContext(config)
}
