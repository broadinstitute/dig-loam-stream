package loamstream.model.execute

import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import scala.concurrent.ExecutionContext
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner {
  def maxNumJobs: Int
  
  def run(jobs: Set[LJob])(implicit context: ExecutionContext): Observable[Map[LJob, Result]]
}
