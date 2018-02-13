package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import rx.lang.scala.Observable
import loamstream.util.Terminable

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner extends Terminable {
  def maxNumJobs: Int
  
  def canRun(job: LJob): Boolean
  
  def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]]
  
  override def stop(): Unit = ()
}
