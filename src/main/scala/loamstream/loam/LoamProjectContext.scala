package loamstream.loam

import loamstream.loam.files.LoamFileManager
import loamstream.util.{DepositBox, ValueBox}
import loamstream.conf.LoamConfig
import loamstream.util.Functions

/** Container for compile time and run time context for a project */
final class LoamProjectContext(val config: LoamConfig, val graphBox: ValueBox[LoamGraph]) {

  def graph: LoamGraph = graphBox.value

  val fileManager: LoamFileManager = new LoamFileManager
  
  val graphsSoFar: ValueBox[Seq[() => LoamGraph]] = ValueBox(Vector.empty)
  
  def registerGraphSoFar(): Unit = {
    val g = graph
    
    val thunk = () => g
    
    graphsSoFar.mutate(_ :+ thunk)
  }
  
  def registerLoamThunk(loamCodeBlock: => Any): Unit = {
    val thunk: () => LoamGraph = Functions.memoize { () =>
      graphBox.get { _ =>
        loamCodeBlock
        
        graph
      }
    }
    
    graphsSoFar.mutate(_ :+ thunk)
  }
}

/** Container for compile time and run time context for a project */
object LoamProjectContext {

  val depositBox = DepositBox.empty[LoamProjectContext]

  def empty(config: LoamConfig): LoamProjectContext = new LoamProjectContext(config, ValueBox(LoamGraph.empty))

}
