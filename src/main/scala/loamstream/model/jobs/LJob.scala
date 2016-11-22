package loamstream.model.jobs

import java.nio.file.Path

import loamstream.util.{Futures, Loggable, ValueBox}
import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject

import scala.concurrent.{ExecutionContext, Future}
import loamstream.model.execute.ExecutionEnvironment


/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LJob extends Loggable {
  def executionEnvironment: ExecutionEnvironment// = ExecutionEnvironment.Local
  
  def workDirOpt: Option[Path] = None

  def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
    val indentString = s"${"-" * indent} >"

    doPrint(s"$indentString ${this}")

    inputs.foreach(_.print(indent + 2, doPrint))
  }

  def name: String = ""

  /** Any jobs this job depends on */
  def inputs: Set[LJob]

  /** Any outputs produced by this job */
  def outputs: Set[Output]

  protected val stateRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)

  /** This job's current state */
  final def state: JobState = stateRef.value

  final val stateEmitter = PublishSubject[JobState]

  lazy val states: Observable[JobState] = stateEmitter.share

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

    notStarted && dependenciesDone && inputsSuccessful
  }

  /**
    * If inputs to this job are available - that is, all jobs that have to happen before this one succeeded.
    */
  def inputsSuccessful: Boolean = inputs.isEmpty || inputs.forall(_.state.isSuccess)

  /**
    * If all jobs that have to happen before this one finished one way or another, either by succeeding or failing.
    */
  def inputsFinished: Boolean = inputs.isEmpty || inputs.forall(_.isFinished)

  /**
    * A job is "Finished" if:
    *   state.isFinished is true and all its inputs' states .isFinisheds are true
    * OR
    * Any input has failed, which prevents this job from running
    */
  def isFinished: Boolean = {
    val anyInputPreventsRunning = inputs.exists(_.state.isFailure)

    def weAreFinished = state.isFinished && inputsFinished

    anyInputPreventsRunning || weAreFinished
  }

  /**
    * Decorates executeSelf(), updating and emitting the value of 'state' from
    * Running to Succeeded/Failed.
    */
  final def execute(implicit context: ExecutionContext): Future[JobState] = {
    import Futures.Implicits._

    updateAndEmitJobState(JobState.NotStarted)
    updateAndEmitJobState(JobState.Running)

    executeSelf.withSideEffect(updateAndEmitJobState)
  }

  /**
    * Implementions of this method will do any actual work to be performed by this job
    */
  protected def executeSelf(implicit context: ExecutionContext): Future[JobState]

  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) {
      this
    }
    else {
      doWithInputs(newInputs)
    }
  }
}

