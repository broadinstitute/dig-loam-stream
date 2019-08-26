package loamstream.v2

import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import loamstream.util.ValueBox
import loamstream.model.LId
import rx.lang.scala.Observable
import loamstream.util.Observables
import rx.lang.scala.subjects.ReplaySubject

object Context {
  def initial: Context = new Context()
  
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
  
  private[v2] lazy val queue: ToolQueue = new ToolQueue.Default(this)
  
  def addInput(tool: Tool)(input: LId): Unit = state.mutate(_.addInput(tool, input))
  
  def addOutput(tool: Tool)(output: LId): Unit = state.mutate(_.addOutput(tool, output))
  
  def name(tool: Tool, toolName: String): Unit = state.mutate(_.nameTool(tool, toolName))
  
  def nameOf(t: Tool): String = state.value.toolNames.collectFirst { case (n, id) if t.id == id => n }.get
  
  def register[S <: Store](s: S): S = {
    state.mutate(_.addStore(s))
    
    s
  }
  
  def register(tool: Tool): Tool = {
    state.mutate(_.addTool(tool))
    
    tool.snapshots.foreach(queue.enqueue)
    
    tool
  }
  
  def updateState(snapshot: Tool.Snapshot): Unit = state.mutate(_.updateToolState(snapshot))
  
  def allDone(): Boolean = state.get { st =>
    def toolsDefined = st.toolStates.nonEmpty
    def allToolsFinished = st.toolStates.values.forall(_.isTerminal)
    
    val result = st.firstPassComplete && toolsDefined && allToolsFinished

    val toolStateString = st.toolStates.map { case (t, st) => s"  ${t.id} => $st" }.mkString("ToolStates(\n","\n","\n)")
    
    if (result) {
      println("Should stop: TRUE")
    } else {
      val unfinished = st.toolStates.filter { case (_, state) => !state.isTerminal }

      println(s"Should NOT stop; first pass done? ${st.firstPassComplete}, any tools defined? $toolsDefined, all tools finished? $allToolsFinished unfinished: $unfinished")
    }
    
    println(toolStateString)

    result
  }
  
  def finishFirstEvaluationPass(): Unit = state.mutate(_.finishFirstEvaluationPass())
}
