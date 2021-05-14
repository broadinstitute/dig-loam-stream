package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.RunData
import loamstream.util.ValueBox
import loamstream.model.jobs.JobOracle
import monix.reactive.Observable

/**
 * @author clint
 * Oct 12, 2016
 */
final case class MockChunkRunner(delegate: ChunkRunner) extends ChunkRunner {
  override def canRun(job: LJob): Boolean = delegate.canRun(job)
  
  val chunks: ValueBox[Seq[Iterable[LJob]]] = ValueBox(Vector.empty)
  
  val chunksWithSettings: ValueBox[Seq[Iterable[(LJob, Settings)]]] = ValueBox(Vector.empty)

  override def run(
      chunk: Iterable[LJob], 
      jobOracle: JobOracle): Observable[(LJob, RunData)] = {
    
    chunks.mutate(_ :+ chunk)
    
    chunksWithSettings.mutate( _ :+ chunk.map(j => j -> j.initialSettings))

    delegate.run(chunk, jobOracle)
  }
}
