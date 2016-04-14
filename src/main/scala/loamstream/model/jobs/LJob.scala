package loamstream.model.jobs

import loamstream.model.jobs.LJob.Result
import loamstream.util.shot.Shot

import scala.concurrent.{ExecutionContext, Future}

/**
 * LoamStream
 * Created by oliverr on 12/23/2015.
 */
object LJob {

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

  case class SimpleSuccess(successMessage: String) extends Success

  trait Failure extends Result {
    final def isSuccess: Boolean = false
    
    final def isFailure: Boolean = true
    
    def failureMessage: String

    def message: String = "Failure! " + failureMessage
  }

  case class SimpleFailure(failureMessage: String) extends Failure

}

trait LJob {
  def inputs: Set[LJob]

  def execute(implicit context: ExecutionContext): Future[Result]
}
