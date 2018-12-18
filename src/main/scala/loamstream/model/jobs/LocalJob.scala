package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * @author clint
 * Nov 7, 2017
 */
trait LocalJob extends LJob {
  /**
   * Implementions of this method will do any actual work to be performed by this job
   */
  def execute(implicit context: ExecutionContext): Future[RunData]
}
