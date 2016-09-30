package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future }

import loamstream.util.DagHelpers
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.ValueBox
import rx.lang.scala.subjects.PublishSubject

/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */
trait LJob extends Loggable with DagHelpers[LJob] {
  def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
    val indentString = s"${"-" * indent} >"

    doPrint(s"$indentString ${this}")

    inputs.foreach(_.print(indent + 2))
  }

  def name: String = ""

  /**
   * Any jobs this job depends on
   */
  def inputs: Set[LJob]

  /**
   * Any outputs produced by this job
   */
  def outputs: Set[Output]

  protected val stateRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)

  /**
   * This job's current state
   */
  final def state: JobState = stateRef.value

  final val stateEmitter = PublishSubject[JobState]

  final protected def emitJobState(): Unit = stateEmitter.onNext(state)

  final def updateAndEmitJobState(newState: JobState): Unit = {
    debug(s"Status change to $newState for job: ${this}")
    stateRef() = newState
    emitJobState()
  }

  def dependencies: Set[LJob] = Set.empty

  /**                                                            
   * If explicitly specified dependencies are done
   */
  def dependenciesDone: Boolean = dependencies.isEmpty || dependencies.forall(_.state.isSuccess)

  /**
   * If this job can be executed
   */
  def isRunnable: Boolean = {
    val notStarted = state == JobState.NotStarted 
    
    notStarted && dependenciesDone && inputsDone
  }

  /**
   * If inputs to this job are available
   */
  def inputsDone: Boolean = inputs.isEmpty || inputs.forall(_.state.isSuccess)

  /**
   * Decorates executeSelf(), updating and emitting the value of 'state' from
   * Running to Succeeded/Failed.
   */
  final def execute(implicit context: ExecutionContext): Future[JobState] = {
    import Futures.Implicits._
    import JobState._

    stateRef() = Running

    executeSelf.withSideEffect { result =>
      stateRef() = if (result.isSuccess) {
        updateAndEmitJobState(Succeeded)
        Succeeded
      } else {
        updateAndEmitJobState(Failed)
        Failed
      }
    }
  }

  /**
   * Implementions of this method will do any actual work to be performed by this job
   */
  protected def executeSelf(implicit context: ExecutionContext): Future[JobState]

  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }

  final override def isLeaf: Boolean = inputs.isEmpty

  final override def leaves: Set[LJob] = {
    if (isLeaf) { Set(this) }
    else { inputs.flatMap(_.leaves) }
  }
}

