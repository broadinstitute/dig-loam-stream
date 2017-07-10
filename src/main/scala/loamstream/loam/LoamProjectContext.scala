package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import loamstream.compiler.GraphQueue

/** Container for compile time and run time context for a project */
final class LoamProjectContext(
    val config: LoamConfig, 
    private val graphBox: ValueBox[LoamGraph],
    val graphQueue: GraphQueue) {

  def graph: LoamGraph = graphBox.value

  val fileManager: LoamFileManager = new LoamFileManager
  
  def updateGraph(f: LoamGraph => LoamGraph): Unit = graphBox.mutate(f)
  
  def registerGraphSoFar(): Unit = {
    val g = graph
    
    graphQueue.enqueue(() => g)
  }
  
  def registerLoamThunk(loamCodeBlock: => Any): Unit = {
    val thunk: () => LoamGraph = Functions.memoize { () =>
      graphBox.get { _ =>
        loamCodeBlock
        
        graph
      }
    }
    
    graphQueue.enqueue(thunk)
  }
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox = DepositBox.empty[LoamProjectContext]

  def empty(config: LoamConfig): LoamProjectContext = {
    new LoamProjectContext(config, ValueBox(LoamGraph.empty), GraphQueue.empty)
  }

}
