package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import loamstream.model.jobs.JobOracle

/**
 * @author clint
 * Oct 12, 2016
 */
final case class MockChunkRunner(delegate: ChunkRunner, log: ChunkRunnerLog) extends ChunkRunner {
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def canRun(job: LJob): Boolean = delegate.canRun(job)
  
  override def run(chunk: Set[LJob]): Observable[Map[LJob, RunData]] = {
    
    log.log(chunk)

    delegate.run(chunk)
  }
}
