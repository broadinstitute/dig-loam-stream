package loamstream.model.jobs

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NonFatal
import loamstream.model.jobs.LJob.Result
import loamstream.util._
import rx.lang.scala.subjects.PublishSubject

import scala.reflect.runtime.universe.Type

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
    import JobState._
    import Futures.Implicits._

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

object LJob {

  sealed trait Result {
    def isSuccess: Boolean

    def isFailure: Boolean

    def message: String
  }

  object Result {
    def attempt(f: => Result): Result = {
      try {
        f
      } catch {
        case NonFatal(ex) => FailureFromThrowable(ex)
      }
    }
  }

  trait Success extends Result {
    final def isSuccess: Boolean = true

    final def isFailure: Boolean = false

    def successMessage: String

    def message: String = s"Success! $successMessage"
  }

  final case class SimpleSuccess(successMessage: String) extends Success

  /**
   * If a job was skipped for various possible reasons (e.g. its outputs were already present)
   */
  final case class SkippedSuccess(successMessage: String) extends Success

  final case class ValueSuccess[T](value: T, typeBox: TypeBox[T]) extends Success {
    def tpe: Type = typeBox.tpe

    override def successMessage: String = s"Got $value"
  }

  trait Failure extends Result {
    final def isSuccess: Boolean = false

    final def isFailure: Boolean = true

    def failureMessage: String

    override def message: String = s"Failure! $failureMessage"
  }

  final case class SimpleFailure(failureMessage: String) extends Failure

  final case class FailureFromThrowable(cause: Throwable) extends Failure {
    override def failureMessage: String = cause.getMessage
  }
}
