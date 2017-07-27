package loamstream.v2

import loamstream.model.LId
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject

trait ToolQueue {
  def enqueue(tool: Tool.Snapshot): Unit

  def allSnapshots: Observable[Tool.Snapshot]

  def runnables: Observable[Tool]
}

object ToolQueue {
  final class Default(state: ValueBox[EvaluationState]) extends ToolQueue {
    private[this] val snapshotsEmitter: Subject[Tool.Snapshot] = ReplaySubject()

    override def enqueue(snapshot: Tool.Snapshot): Unit = snapshotsEmitter.onNext(snapshot)

    override def allSnapshots: Observable[Tool.Snapshot] = snapshotsEmitter

    override lazy val runnables: Observable[Tool] = {
      val unlimited = for {
        snapshot <- allSnapshots
        _ = println(s"Raw snapshot: $snapshot")
        //_ = updateState(snapshot)
        runnable <- runnablesFrom(snapshot)
      } yield {
        println(s"Runnable: $snapshot")

        snapshot.tool
      }

      def shouldStop(): Boolean = state.get { st =>
        val result = st.toolStates.values.forall(_.isTerminal)

        if (result) {
          println("Should stop: TRUE")
        } else {
          val unfinished = st.toolStates.filter { case (_, state) => !state.isTerminal }

          println(s"Should NOT stop, due to: $unfinished")
        }

        result
      }

      unlimited.takeUntil(_ => shouldStop())
    }

    private def runnablesFrom(snapshot: Tool.Snapshot): Observable[Tool.Snapshot] = state.get { st =>
      val Tool.Snapshot(state, t) = snapshot

      val hasNoInputs = !st.inputs.contains(t)

      def selfRunnable: Observable[Tool.Snapshot] = {
        if (state.isNotStarted) {
          println(s"Declared runnable because not started: $t")

          Observable.just(snapshot)
        } else {
          println(s"Running or already ran: $t")

          Observable.empty
        }
      }

      def depRunnables: Observable[Tool.Snapshot] = {
        def producerOf(storeId: LId): Option[Tool] = {
          st.outputs.collect { case (t, sids) if sids.contains(storeId) => t }.headOption
        }

        val inputStores = st.inputs(t)

        println(s"Input ids: $inputStores for tool: $t")

        val producersOfThoseStores = inputStores.toSeq.flatMap(producerOf)

        println(s"Input store producers: $producersOfThoseStores for tool: $t")

        val producerSnapshots = Observables.merge(producersOfThoseStores.map(_.snapshots))

        producerSnapshots.flatMap(runnablesFrom)
      }

      if (hasNoInputs) { selfRunnable }
      else {
        depRunnables ++ selfRunnable
      }
    }
  }
}
