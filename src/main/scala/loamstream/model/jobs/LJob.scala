package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future, blocking }
import scala.reflect.runtime.universe.Type
import scala.util.control.NonFatal

import loamstream.model.jobs.LJob.Result
import loamstream.util.{DagHelpers, Loggable, TypeBox}
import loamstream.util.Futures
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject



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
  
  /**
   * An observable producing a stream of all the runnable jobs among this job, its dependencies, their dependencies,
   * and so on, as soon as those jobs become runnable.  A job becomes runnable when all its dependencies are finished,
   * or if it has no dependencies, it's runnable immediately.  (See selfRunnable)
   */
  final lazy val runnables: Observable[LJob] = {
    //Multiplex the streams of runnable jobs starting from each of our dependencies
    val dependencyRunnables = {
      if(inputs.isEmpty) { Observable.empty }
      //NB: Note the use of merge instead of ++; this ensures that we don't emit jobs from the sub-graph rooted at
      //one dependency before the other dependencies, but rather emit all the streams of runnable jobs "together".
      else { inputs.toSeq.map(_.runnables).reduce(_ merge _) }
    }
    
    //Emit the current job after all our dependencies
    (dependencyRunnables ++ selfRunnable) 
  }
  
  /**
   * An observable that will emit this job ONLY when all this job's dependencies are finished.
   * If the this job has no dependencies, this job is emitted immediately.  This will fire at most once.
   */
  private lazy val selfRunnable: Observable[LJob] = {
    if(inputs.isEmpty) { 
      Observable.just(this)
    } else {
      //An observable that will emit a sequence containing all our dependencies' "terminal" states.
      //When this fires, our dependencies are finished.
      val lastInputStates: Observable[Seq[JobState]] = Observables.sequence(inputs.toSeq.map(_.lastState))
      
      for {
        states <- lastInputStates
      } yield {
        debug(s"$name.selfRunnable: deps finished with states: $states")
        
        this
      }
    }
  }
  
  private[this] val stateRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)

  /**
   * This job's current state.
   */
  final def state: JobState = stateRef.value

  //NB: Needs to be a ReplaySubject for correct operation
  private[this] val stateEmitter: Subject[JobState] = ReplaySubject[JobState]()

  /**
   * An observable stream of states emitted by this job, each one reflecting a state this job transitioned to.
   */
  final def states: Observable[JobState] = stateEmitter

  /**
   * The "terminal" state emitted by this job: the one that indicates the job is finished for any reason.
   * Will fire at most one time. 
   */
  private lazy val lastState: Observable[JobState] = states.filter(_.isFinished).first
  
  /**
   * Sets the state of this job to be newState, and emits the new state to any observers.
   * @param newState the new state to set for this job 
   */
  //NB: Currently needs to be public for use in UgerChunkRunner :\
  final def updateAndEmitJobState(newState: JobState): Unit = {
    trace(s"Status change to $newState for job: ${this}")
    stateRef() = newState
    stateEmitter.onNext(newState)
  }

  /**
   * Decorates executeSelf, ensuring that job state change events are emitted.
   */
  final def execute(implicit context: ExecutionContext): Future[Result] = {
    import Futures.Implicits._
    import JobState._
    
    updateAndEmitJobState(Running)
    
    executeSelf.withSideEffect { result =>
      updateAndEmitJobState(if(result.isSuccess) Succeeded else Failed)
    }
  }
  
  /**
   * Will do any actual work meant to be performed by this job
   */
  protected def executeSelf(implicit context: ExecutionContext): Future[Result]

  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }

  final override def isLeaf: Boolean = inputs.isEmpty

  final override def leaves: Set[LJob] = {
    if (isLeaf) {
      Set(this)
    }
    else {
      inputs.flatMap(_.leaves)
    }
  }

  final def remove(input: LJob): LJob = {
    if ((input eq this) || isLeaf) {
      this
    }
    else {
      val newInputs = (inputs - input).map(_.remove(input))

      withInputs(newInputs)
    }
  }

  final override def removeAll(toRemove: Iterable[LJob]): LJob = {
    toRemove.foldLeft(this)(_.remove(_))
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
