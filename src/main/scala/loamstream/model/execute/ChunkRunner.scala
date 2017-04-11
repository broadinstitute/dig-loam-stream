package loamstream.model.execute

import loamstream.model.jobs.{Execution, LJob}
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner {
  def maxNumJobs: Int
  
  def canRun(job: LJob): Boolean
  
  //TODO
  def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, Execution]]
}
