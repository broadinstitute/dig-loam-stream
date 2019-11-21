package loamstream.model.execute

import loamstream.util.ValueBox
import loamstream.model.jobs.LJob

final class ChunkRunnerLog {
  val chunks: ValueBox[Seq[Set[LJob]]] = ValueBox(Vector.empty)
  
  val chunksWithSettings: ValueBox[Seq[Set[(LJob, Settings)]]] = ValueBox(Vector.empty) 
  
  def log(chunk: Set[LJob]): Unit = {
    chunks.mutate(_ :+ chunk)
    
    chunksWithSettings.mutate( _ :+ chunk.map(j => j -> j.initialSettings))
  }
}
