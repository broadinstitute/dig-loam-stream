package loamstream.model.execute

import loamstream.model.jobs.LJob

/**
 * @author clint
 * Nov 28, 2016
 */
abstract class ChunkRunnerFor(val environmentType: EnvironmentType) extends ChunkRunner {
  override def canRun(job: LJob): Boolean = job.initialSettings.envType == environmentType
}
