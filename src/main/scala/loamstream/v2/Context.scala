package loamstream.v2

import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import loamstream.util.ValueBox
import loamstream.model.LId
import rx.lang.scala.Observable
import loamstream.util.Observables
import rx.lang.scala.subjects.ReplaySubject

object Context {
  private def emptyMap: Map[Tool, Set[LId]] = Map.empty.withDefaultValue(Set.empty)
}

final class Context {
  override def toString: String = state.get { st =>
    s"""|Context(
        |  queue:      $queue
        |  sympols:    ${st.symbols}
        |  inputs:     ${st.inputs}
        |  outputs:    ${st.outputs}
        |  toolStates: ${st.toolStates}
        |)""".stripMargin
  }
  
  private[v2] val state: ValueBox[EvaluationState] = ValueBox(EvaluationState.Initial)
  
  private[v2] lazy val queue: ToolQueue = new ToolQueue.Default(state)
  
  def addInput(tool: Tool)(input: LId): Unit = state.mutate(_.addInput(tool, input))
  
  def addOutput(tool: Tool)(output: LId): Unit = state.mutate(_.addOutput(tool, output))
  
  def register(s: Store): Store = {
    state.mutate(_.addStore(s))
    
    s
  }
  
  def register(tool: Tool): Tool = {
    state.mutate(_.addTool(tool))
    
    tool.snapshots.foreach(queue.enqueue)
    
    tool
  }
  
  queue.allSnapshots.foreach(updateState)
  
  private def updateState(snapshot: Tool.Snapshot): Unit = state.mutate(_.updateToolState(snapshot))
  
  
}
