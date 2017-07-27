package loamstream.v2

import loamstream.model.LId
import loamstream.util.ValueBox
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.Observable

trait Tool extends LId.Owner {
  final override lazy val id: LId = LId.newAnonId 
  
  private[this] val stateBox: ValueBox[ToolState] = ValueBox(ToolState.NotStarted)
  
  def state: ToolState = stateBox.value
  
  private[this] val snapshotSubject: Subject[Tool.Snapshot] = ReplaySubject()
  
  def snapshots: Observable[Tool.Snapshot] = snapshotSubject
  
  private[this] def snapshot(st: ToolState): Tool.Snapshot = Tool.Snapshot(st, this)
  
  private[this] def init(): Unit = {
    val s = snapshot(ToolState.NotStarted)
    
    println(s"Emitting initial snapshot: $s")
    
    snapshotSubject.onNext(s)
  }
  
  init()
  
  final def transitionTo(newState: ToolState): ToolState = {
    println(s"Transitioning to: $newState: $this")
    
    stateBox.get { oldState =>
      if(newState == oldState) { 
        println(s"No transition to do: $this")
        
        oldState 
      } else {
        stateBox := newState
        snapshotSubject.onNext(snapshot(newState))
        
        if(newState.isTerminal) { 
          snapshotSubject.onCompleted()
        }
        
        println(s"Transitioned to: $newState from $oldState: $this")
        
        newState
      }
    }
  }
  
  def context: Context
  
  def referencedStores: Set[LId]
  
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
  final case class Snapshot(state: ToolState, tool: Tool)
}
