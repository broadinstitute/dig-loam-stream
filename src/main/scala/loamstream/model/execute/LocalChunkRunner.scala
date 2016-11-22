package loamstream.model.execute

import loamstream.model.jobs.LJob

/**
 * @author clint
 * Nov 22, 2016
 */
trait LocalChunkRunner extends ChunkRunner {
  final override def canRun(job: LJob): Boolean = job.executionEnvironment.isLocal
}