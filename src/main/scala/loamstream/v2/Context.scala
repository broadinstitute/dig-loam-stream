package loamstream.v2

import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import loamstream.util.ValueBox
import loamstream.model.LId
import rx.lang.scala.Observable
import loamstream.util.Observables
import rx.lang.scala.subjects.ReplaySubject

final class Context {
  override def toString: String = {
    s"""|Context(
        |  queue:      $queue
        |  sympols:    ${symbols()}
        |  inputs:     ${inputs()}
        |  outputs:    ${outputs()}
        |  toolStates: ${toolStates()}
        |)""".stripMargin
  }
  
  private[v2] val queue: ToolQueue = new ToolQueue
  
  private[v2] val symbols: ValueBox[SymbolTable] = ValueBox(SymbolTable.Empty)
  
  private[this] val inputs: ValueBox[Map[Tool, Set[LId]]] = ValueBox(emptyMap)
    
  private[this] val outputs: ValueBox[Map[Tool, Set[LId]]] = ValueBox(emptyMap)

  private[this] val toolStates: ValueBox[Map[Tool, ToolState]] = ValueBox(Map.empty)
  
  private[this] def emptyMap: Map[Tool, Set[LId]] = Map.empty.withDefaultValue(Set.empty)
  
  def addInput(tool: Tool)(input: LId): Unit = inputs.mutate { inputsByTool =>
    val inputsSoFar = inputsByTool(tool)
    
    inputsByTool + (tool -> (inputsSoFar + input))
  }
  
  def addOutput(tool: Tool)(output: LId): Unit = outputs.mutate { outputsByTool =>
    val outputsSoFar = outputsByTool(tool)
    
    outputsByTool + (tool -> (outputsSoFar + output))
  }
  
  def register(s: Store): Unit = symbols.mutate(_ + s)
  
  def register(tool: Tool): Unit = {
    symbols.mutate(_ + tool)
    
    tool.snapshots.foreach(allSnapshotsSubject.onNext)
  }
  
  private def using[A,B,C](vb0: ValueBox[A], vb1: ValueBox[B])(f: (A, B) => C): C = {
    vb0.get { a =>
      vb1.get { b =>
        f(a,b)
      }
    }
  }
  
  def runnablesFrom(snapshot: Tool.Snapshot): Observable[Tool.Snapshot] = using(inputs, outputs) { (toolsToInputs, toolsToOutputs) =>
    val Tool.Snapshot(state, t) = snapshot
    
    val hasNoInputs = !toolsToInputs.contains(t)
    
    def selfRunnable: Observable[Tool.Snapshot] = {
      if(state.isNotStarted) {
        println(s"Declared runnable because not started: $t")
        
        Observable.just(snapshot) 
      } else { 
        println(s"Running or already ran: $t")
        
        Observable.empty 
      }
    }
    
    def depRunnables: Observable[Tool.Snapshot] = {
      def producerOf(storeId: LId): Option[Tool] = {
        toolsToOutputs.collect { case (t, sids) if sids.contains(storeId) => t }.headOption
      }
      
      val inputStores = toolsToInputs(t)
      
      println(s"Input ids: $inputStores for tool: $t")
      
      val producersOfThoseStores = inputStores.toSeq.flatMap(producerOf)
      
      println(s"Input store producers: $producersOfThoseStores for tool: $t")
      
      val producerSnapshots = Observables.merge(producersOfThoseStores.map(_.snapshots))
      
      producerSnapshots.flatMap(runnablesFrom)
    }
    
    if(hasNoInputs) { selfRunnable } 
    else {
      depRunnables ++ selfRunnable
    }
  }
  
  private[this] val allSnapshotsSubject: Subject[Tool.Snapshot] = ReplaySubject()
  
  allSnapshotsSubject.foreach(updateState)
  
  private def updateState(snapshot: Tool.Snapshot): Unit = {
    toolStates.mutate(_ + (snapshot.tool -> snapshot.state))
  }
  
  lazy val runnables: Observable[Tool] = {
    val unlimited = for {
      snapshot <- allSnapshotsSubject
      _ = println(s"Raw snapshot: $snapshot")
      //_ = updateState(snapshot)
      runnable <- runnablesFrom(snapshot)
    } yield {
      println(s"Runnable: $snapshot")
      
      snapshot.tool
    }
    
    def shouldStop(): Boolean = toolStates.get { states =>
      val result = states.values.forall(_.isTerminal)
      
      if(result) {
        println("Should stop: TRUE")
      } else {
        println(s"Should NOT stop, due to: ${states.filter { case (_, state) => !state.isTerminal }}")
      }
      
      result
    }
    
    unlimited.takeUntil(_ => shouldStop())
  }
}
