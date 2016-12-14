package loamstream.model.jobs

import java.nio.file.Path

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.ExecutionEnvironment
import loamstream.util.Loggable
import loamstream.util.Observables
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.ReplaySubject


/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */
trait LJob extends Loggable {
  def executionEnvironment: ExecutionEnvironment// = ExecutionEnvironment.Local
  
  def workDirOpt: Option[Path] = None
  
  def print(indent: Int = 0, doPrint: String => Unit = debug(_)): Unit = {
    val indentString = s"${"-" * indent} >"

    doPrint(s"$indentString ($state)${this}")

    inputs.foreach(_.print(indent + 2, doPrint))
  }

  def name: String = ""

  /** Any jobs this job depends on */
  def inputs: Set[LJob]

  /** Any outputs produced by this job */
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
      else { Observables.merge(inputs.toSeq.map(_.runnables)) }
    }
    
    //Emit the current job *after* all our dependencies
    (dependencyRunnables ++ selfRunnable) 
  }
  
  /**
   * An observable that will emit this job ONLY when all this job's dependencies are finished.
   * If the this job has no dependencies, this job is emitted immediately.  This will fire at most once.
   */
  private[jobs] lazy val selfRunnable: Observable[LJob] = {
    def justUs = Observable.just(this)
    def noMore = Observable.empty 
    
    if(inputs.isEmpty) { 
      justUs
    } else {
      for {
        inputStates <- finalInputStates
        _ = debug(s"$name.selfRunnable: deps finished with states: $inputStates")
        anyInputFailures = inputStates.exists(_.isFailure)
        runnable <- if(anyInputFailures) noMore else justUs 
      } yield {
        runnable
      }
    }
  }
  
  private[this] val stateRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)

  /** This job's current state */
  final def state: JobState = stateRef.value

  /**
   * An observable stream of states emitted by this job, each one reflecting a state this job transitioned to.
   */
  //NB: Needs to be a ReplaySubject for correct operation
  private[this] val stateEmitter: Subject[JobState] = ReplaySubject[JobState]()
  
  lazy val states: Observable[JobState] = stateEmitter

  final protected def emitJobState(): Unit = stateEmitter.onNext(state)
  
  /**
   * The "terminal" state emitted by this job: the one that indicates the job is finished for any reason.
   * Will fire at most one time. 
   */
  protected[jobs] lazy val lastState: Observable[JobState] = states.filter(_.isFinished).first
  
  /**
   * An observable that will emit a sequence containing all our dependencies' "terminal" states.
   * When this fires, our dependencies are finished.
   */
  protected[jobs] lazy val finalInputStates: Observable[Seq[JobState]] = {
    Observables.sequence(inputs.toSeq.map(_.lastState))
  }
  
  /**
   * Sets the state of this job to be newState, and emits the new state to any observers.
   * @param newState the new state to set for this job 
   */
  //NB: Currently needs to be public for use in UgerChunkRunner :\
  final def updateAndEmitJobState(newState: JobState): Unit = {
    debug(s"Status change to $newState for job: ${this}")
    stateRef() = newState
    stateEmitter.onNext(newState)
  }

  /**
   * Decorates executeSelf(), updating and emitting the value of 'state' from
   * Running to Succeeded/Failed.
   */
  def execute(implicit context: ExecutionContext): Future[JobState] = {
    import loamstream.util.Futures.Implicits._

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
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }
}

