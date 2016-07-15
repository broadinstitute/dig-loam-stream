package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future, blocking }

import loamstream.model.jobs.LJob.Result
import loamstream.util.DagHelpers
import loamstream.util.Loggable
import scala.util.control.NonFatal

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

  protected def doWithInputs(newInputs: Set[LJob]): LJob
  
  final def withInputs(newInputs: Set[LJob]): LJob = {
    if(inputs == newInputs) { this }
    else { doWithInputs(newInputs) }
  }
  
  def execute(implicit context: ExecutionContext): Future[Result]
  
  protected def runBlocking[R <: Result](f: => R)(implicit context: ExecutionContext): Future[R] = Future(blocking(f))
  
  final override def isLeaf: Boolean = inputs.isEmpty
  
  final override def leaves: Set[LJob] = {
    if(isLeaf) { Set(this) }
    else { inputs.flatMap(_.leaves) }
  }

  def remove(input: LJob): LJob = {
    if((input eq this) || isLeaf) { this }
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
      try { f } catch {
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
