package loamstream.model.execute

import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner {
  def maxNumJobs: Int

  def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]]
}
