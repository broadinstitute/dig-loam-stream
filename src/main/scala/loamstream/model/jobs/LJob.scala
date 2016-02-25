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
    def message: String
  }

  trait Success extends Result {
    def successMessage: String

    def message: String = "Success! " + successMessage
  }

  trait Failure extends Result {
    def failureMessage: String

    def message: String = "Failure! " + failureMessage
  }

}

trait LJob {
  def inputs: Set[LJob]

  def execute(implicit context: ExecutionContext): Shot[Future[Result]]
}
