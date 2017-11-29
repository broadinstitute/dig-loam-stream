package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import loamstream.compiler.GraphQueue
import loamstream.model.Tool

/** Container for compile time and run time context for a project */
final class LoamProjectContext(
    val config: LoamConfig, 
    private val graphBox: ValueBox[LoamGraph],
    val graphQueue: GraphQueue) {

  def graph: LoamGraph = graphBox.value

  val fileManager: LoamFileManager = new LoamFileManager
  
  def updateGraph(f: LoamGraph => LoamGraph): Unit = graphBox.mutate(f)
  
  /**
   * Enqueue a graph thunk that returns the graph as of when this method was called. 
   */
  def registerGraphSoFar(): Unit = {
    val g = graph
    
    graphQueue.enqueue(() => g)
  }
  
  /**
   * Enqueue a graph thunk that returns the graph as of immediately after running `loamCodeBlock`
   */
  def registerLoamThunk(loamCodeBlock: => Any): Unit = {
    //NB: Memoize the thunk, so the loam code block is only run once, 
    //not every time .apply() is called on the thunk.
    val thunk: () => LoamGraph = Functions.memoize { () =>
      //NB: Lock on graphBox, to ensure that no one mutates graphBox between running `loamCodeBlock` and
      //getting the latest graph 
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
