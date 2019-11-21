package loamstream.model.execute

import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.Terminable
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 16, 2016
 */
trait ChunkRunner extends Terminable {
  def maxNumJobs: Int
  
  def canRun(job: LJob): Boolean
  
  def run(jobs: Set[LJob]): Observable[Map[LJob, RunData]]
  
  override def stop(): Iterable[Throwable] = Nil
}

object ChunkRunner {
  type Constructor[R <: ChunkRunner] = (LJob => Boolean, JobOracle) => R
}
