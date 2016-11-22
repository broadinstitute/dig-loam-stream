package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner {
  def maxNumJobs: Int
  
  def canRun(job: LJob): Boolean = true
  
  def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]]
}
