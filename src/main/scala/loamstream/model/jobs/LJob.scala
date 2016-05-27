package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future, blocking }

import loamstream.model.jobs.LJob.Result
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */
trait LJob {
  @deprecated("", "")
  def print(indent: Int = 0): Unit = {
    val indentString = s"${"-" * indent} >"
    
    println(s"$indentString ${this}")
    
    inputs.foreach(_.print(indent + 2))
  }
  
  def inputs: Set[LJob]

  def withInputs(newInputs: Set[LJob]) : LJob
  
  def execute(implicit context: ExecutionContext): Future[Result]
  
  final def isLeaf: Boolean = inputs.isEmpty
  
  protected def runBlocking(f: => Result)(implicit context: ExecutionContext): Future[Result] = Future(blocking(f))
}

object LJob {

  sealed trait Result {
    def isSuccess: Boolean
    
    def isFailure: Boolean
    
    def message: String
  }
  
  object Result {
    def attempt(f: => Result): Result = {
      import scala.{ util => su }
      
      su.Try(f) match {
        case su.Success(r) => r
        case su.Failure(ex) => SimpleFailure(ex.getMessage)
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
