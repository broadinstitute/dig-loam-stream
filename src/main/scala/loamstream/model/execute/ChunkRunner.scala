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
  def canRun(job: LJob): Boolean
  
  def run(jobs: Set[LJob], jobOracle: JobOracle): Observable[(LJob, RunData)]
  
  override def stop(): Unit = ()
}
