package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * @author clint
 * Nov 7, 2017
 */
@deprecated(
    message = "A shim to make mock jobs used in unit tests work; TODO: move to a typeclass-driven approach", 
    since = "")
trait LocalJob extends LJob {
  /**
   * Implementions of this method will do any actual work to be performed by this job
   */
  def execute(implicit context: ExecutionContext): Future[RunData]
}
