package loamstream.model.execute

import loamstream.model.jobs.JobResult
import loamstream.model.jobs.LJob
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner {
  def maxNumJobs: Int
  
  def canRun(job: LJob): Boolean
  
  def run(jobs: Set[LJob]): Observable[Map[LJob, JobResult]]
}
