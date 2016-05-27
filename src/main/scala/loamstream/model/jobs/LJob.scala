package loamstream.model.jobs

import scala.concurrent.{ ExecutionContext, Future, blocking }

import loamstream.model.jobs.LJob.Result

/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */
trait LJob {
  @deprecated("", "")
  def print(indent: Int = 0): Unit = {
    val indentString = s"${"-" * indent} >"
    
    //println(s"$indentString ${getClass.getName}")
    println(s"$indentString ${this}")
    
    inputs.foreach(_.print(indent + 2))
  }
  
  def inputs: Set[LJob]

  def withInputs(newInputs: Set[LJob]) : LJob
  
  def execute(implicit context: ExecutionContext): Future[Result]
  
  final def isLeaf: Boolean = inputs.isEmpty
}

object LJob {

  trait Helpers { self: LJob =>
    protected def runBlocking(f: => Result)(implicit context: ExecutionContext): Future[Result] = {
      Future(blocking(f))
    }
  }
  
  sealed trait Result {
    def isSuccess: Boolean
    
    def isFailure: Boolean
    
    def message: String
  }

  trait Success extends Result {
    final def isSuccess: Boolean = true
    
    final def isFailure: Boolean = false
    
    def successMessage: String

    def message: String = "Success! " + successMessage
  }

  final case class SimpleSuccess(successMessage: String) extends Success

  trait Failure extends Result {
    final def isSuccess: Boolean = false
    
    final def isFailure: Boolean = true
    
    def failureMessage: String

    def message: String = "Failure! " + failureMessage
  }

  final case class SimpleFailure(failureMessage: String) extends Failure
}
