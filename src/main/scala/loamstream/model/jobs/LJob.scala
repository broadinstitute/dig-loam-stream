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
   * Returns an iterator that does a post-order traversal of this tree
   */
  def iterator: Iterator[LJob] = postOrder
  
  /**
   * Returns an iterator that does a post-order traversal of this tree; that is, 
   * this node's children (dependencies/inputs) are visited before this node.  
   */
  def postOrder: Iterator[LJob] = childIterator(_.postOrder) ++ Iterator.single(this)
  
  /**
   * Returns an iterator that does a pre-order traversal of this tree; that is, 
   * this node is visited before its children (dependencies/inputs).  
   */
  def preOrder: Iterator[LJob] = Iterator.single(this) ++ childIterator(_.preOrder)
  
  private def childIterator(iterationStrategy: LJob => Iterator[LJob]): Iterator[LJob] = {
    inputs.iterator.flatMap(iterationStrategy)
  }

  final lazy val runnables: Observable[LJob] = {
    val inputRunnables = {
      if(inputs.isEmpty) { Observable.empty }
      else { inputs.toSeq.map(_.runnables).reduce(_ merge _) }
    }
    
    (inputRunnables ++ selfRunnable) 
  }
  
  private lazy val lastInputStates: Observable[Seq[JobState]] = {
    if(inputs.isEmpty) { Observable.just(Nil) }
    else { Observables.sequence(inputs.toSeq.map(_.lastState)) }
  }
  
  private lazy val selfRunnable: Observable[LJob] = {
    //info(s"$name.selfRunnable: *****BEGIN*****")
    
    if(inputs.isEmpty) { 
      //info(s"$name.selfRunnable: *****END*****")
      
      Observable.just(this)
    } else {
      val result = for {
        states <- lastInputStates
        _ = info(s"$name.selfRunnable: deps finished with states: $states")
      } yield this
      
      //info(s"$name.selfRunnable: *****END*****")
    
      result
    }
  }
  
  final private[this] val stateRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)

  /**
   * This job's current state
   */
  final def state: JobState = stateRef.value

  //NB: Needs to be a ReplaySubject
  final private[this] val stateEmitter: Subject[JobState] = ReplaySubject[JobState]()

  def states: Observable[JobState] = stateEmitter
  
  private lazy val lastState: Observable[JobState] = states.filter(_.isFinished).first
  
  /*lastState.foreach { st =>
    info(s"$name: LAST state: $st")
  }*/
  
  final def updateAndEmitJobState(newState: JobState): Unit = {
    trace(s"Status change to $newState for job: ${this}")
    stateRef() = newState
    stateEmitter.onNext(newState)
  }

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

  protected def runBlocking[R <: Result](f: => R)(implicit context: ExecutionContext): Future[R] = Future(blocking(f))

  final override def isLeaf: Boolean = inputs.isEmpty

  final override def leaves: Set[LJob] = {
    if (isLeaf) {
      Set(this)
    }
    else {
      inputs.flatMap(_.leaves)
    }
  }

  def remove(input: LJob): LJob = {
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
