package loamstream.v2

import loamstream.model.LId
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject
import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.Subscription

trait ToolQueue {
  def enqueue(tool: Tool.Snapshot): Unit

  def allSnapshots: Observable[Tool.Snapshot]

  def runnables: Observable[Tool]
  
  def start(): Unit
  
  def stop(): Unit
}

object ToolQueue {
  final class Default(context: Context) extends ToolQueue {
    private[this] val snapshotsEmitter: Subject[Tool.Snapshot] = ReplaySubject()

    override def enqueue(snapshot: Tool.Snapshot): Unit = snapshotsEmitter.onNext(snapshot)

    override lazy val allSnapshots: Observable[Tool.Snapshot] = snapshotsEmitter.share
    
    override def start(): Unit = context.finishFirstEvaluationPass()
    
    override def stop(): Unit = snapshotsEmitter.onCompleted()
    
    override lazy val runnables: Observable[Tool] = {
      val unlimited = for {
        snapshot <- allSnapshots
        _ = println(s"Raw snapshot: $snapshot")
        runnable <- runnablesFrom(snapshot)
      } yield {
        println(s"Runnable: $snapshot")

        snapshot.tool
      }

      try {
        unlimited.takeUntil(_ => context.allDone()).publish.autoConnect
      } finally {
        start()
      }
    }

    private def runnablesFrom(snapshot: Tool.Snapshot): Observable[Tool.Snapshot] = {
      val Tool.Snapshot(state, tool) = snapshot

      def evaluationState = context.state.value
      
      def selfRunnable: Observable[Tool.Snapshot] = {
        if (state.isNotStarted) {
          println(s"Declared runnable because not started: $tool")

          Observable.just(snapshot)
        } else {
          println(s"Running or already ran: $tool")

          Observable.empty
        }
      }

      def depRunnables: Observable[Tool.Snapshot] = {
        def producerOf(storeId: LId): Option[Tool] = {
          evaluationState.outputs.collect { case (t, storeIds) if storeIds.contains(storeId) => t }.headOption
        }

        val directInputStoreIds = evaluationState.inputs(tool)
        val ancestorInputStoreIds = directInputStoreIds.map(evaluationState.symbols.resolveStore).flatMap(_.ancestors).map(_.id)
        
        val inputStoreIds = directInputStoreIds ++ ancestorInputStoreIds

        println(s"Input ids: $inputStoreIds for tool: $tool")

        val producersOfThoseStores = inputStoreIds.flatMap(producerOf)

        println(s"Input store producers: $producersOfThoseStores for tool: $tool")

        val producerSnapshots = Observables.merge(producersOfThoseStores.toSeq.map(_.snapshots))

        producerSnapshots.flatMap(runnablesFrom)
      }

      val hasNoInputs = !evaluationState.inputs.contains(tool)
      
      if (hasNoInputs) { selfRunnable }
      else {
        depRunnables ++ selfRunnable
      }
    }
  }
}
