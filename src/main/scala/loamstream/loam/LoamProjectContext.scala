package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}
import loamstream.conf.LoamConfig
import loamstream.util.Functions
import loamstream.compiler.GraphQueue

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val config: LoamConfig, private val stateBox: ValueBox[LoamProjectState]) {

  def state: LoamProjectState = stateBox.value
  
  def graphsSoFar: GraphQueue = state.graphsSoFar
  
  def graph: LoamGraph = state.graph

  val fileManager: LoamFileManager = new LoamFileManager
  
  def updateGraph(f: LoamGraph => LoamGraph): Unit = stateBox.mutate(_.mapGraph(f))
  
  def registerGraphSoFar(): Unit = {
    val g = graph
    
    enqueueThunk(() => g)
  }
  
  def registerLoamThunk(loamCodeBlock: => Any): Unit = {
    val thunk: () => LoamGraph = Functions.memoize { () =>
      stateBox.get { state =>
        loamCodeBlock
        
        graph
      }
    }
    
    enqueueThunk(thunk)
  }
  
  private def enqueueThunk(thunk: () => LoamGraph): Unit = stateBox.mutate(_.mapGraphsSoFar(_.enqueue(thunk)))
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox = DepositBox.empty[LoamProjectContext]

  import LoamProjectState.{ initial => initialProjectState }
  
  def empty(config: LoamConfig): LoamProjectContext = new LoamProjectContext(config, ValueBox(initialProjectState))

}
