package loamstream.v2

import loamstream.model.LId
import loamstream.util.ValueBox
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.Observable

trait Tool extends LId.HasId {
  final override lazy val id: LId = LId.newAnonId 
  
  private[this] val snapshotSubject: Subject[Tool.Snapshot] = ReplaySubject()
  
  def snapshots: Observable[Tool.Snapshot] = snapshotSubject
  
  private[v2] def snapshot(st: ToolState): Tool.Snapshot = Tool.Snapshot(st, this)
  
  private[this] def init(): Unit = {
    val s = snapshot(ToolState.NotStarted)
    
    println(s"Emitting initial snapshot: $s")
    
    snapshotSubject.onNext(s)
  }
  
  init()
  
  private def emitSnapshot(newSnapshot: Tool.Snapshot): Unit = {
    snapshotSubject.onNext(newSnapshot)
    
    if(newSnapshot.state.isTerminal) { 
      snapshotSubject.onCompleted()
    }
  }
  
  final def transitionTo(newToolState: ToolState): ToolState = {
    println(s"Transitioning to: $newToolState: $this")
    
    val newSnapshot = snapshot(newToolState)
    
    context.updateState(newSnapshot)
    
    emitSnapshot(newSnapshot)
    
    newToolState
  }
  
  def context: Context
  
  def referencedStores: Set[LId]
  
  def named(name: String): Tool = {
    context.name(this, name)
    
    this
  }
  
  def in(store: Store, rest: Store*): Tool = in(store +: rest)
  
  def in(stores: Seq[Store]): Tool = {
    stores.foreach(s => context.addInput(this)(s.id))
    
    this
  }
  
  def out(store: Store, rest: Store*): Tool = out(store +: rest)
  
  def out(stores: Seq[Store]): Tool = {
    stores.foreach(s => context.addOutput(this)(s.id))
    
    this
  }
}

object Tool {
  final case class Snapshot(state: ToolState, tool: Tool) {
    def asTuple: (Tool, ToolState) = (tool, state)
  }
}
