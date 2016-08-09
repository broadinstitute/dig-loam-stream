package loamstream.model.jobs

import scala.concurrent.{ExecutionContext, Future, blocking}
import loamstream.model.jobs.LJob.Result
import loamstream.util.{DagHelpers, Loggable, TypeBox}

import scala.util.control.NonFatal
import loamstream.util.ValueBox
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

  def inputs: Set[LJob]

  def outputs: Set[Output]
  
  private val statusRef: ValueBox[JobState] = ValueBox(JobState.NotStarted)
  
  final def status: JobState = statusRef.value
  
  final protected def isSuccess: Boolean = status.isFinished
  
  final def execute(implicit context: ExecutionContext): Future[Result] = {
    val f = executeSelf
    
    import JobState._
    
    f.foreach { result =>
      statusRef() = if(result.isSuccess) Finished else Failed
    }
    
    f
  }
  
  protected def executeSelf(implicit context: ExecutionContext): Future[Result]
  
  protected def doWithInputs(newInputs: Set[LJob]): LJob

  final def withInputs(newInputs: Set[LJob]): LJob = {
    if (inputs eq newInputs) { this }
    else { doWithInputs(newInputs) }
  }
  
  protected def doWithOutputs(newOutputs: Set[Output]): LJob
  
  final def withOutputs(newOutputs: Set[Output]): LJob = {
   if (outputs eq newOutputs) { this }
    else { doWithOutputs(newOutputs) } 
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
