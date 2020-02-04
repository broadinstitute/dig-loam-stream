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
final case class MockChunkRunner(delegate: ChunkRunner) extends ChunkRunner {
  override def maxNumJobs: Int = delegate.maxNumJobs
  
  override def canRun(job: LJob): Boolean = delegate.canRun(job)
  
  val chunks: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)
  
  val chunksWithSettings: ValueBox[Seq[Set[(LJob, Settings)]]] = ValueBox(Vector.empty)

  override def run(
      chunk: Set[LJob], 
      jobOracle: JobOracle): Observable[Map[LJob, RunData]] = {
    
    chunks.mutate(_ :+ chunk)
    
    chunksWithSettings.mutate( _ :+ chunk.map(j => j -> j.initialSettings))

    delegate.run(chunk, jobOracle)
  }
}
