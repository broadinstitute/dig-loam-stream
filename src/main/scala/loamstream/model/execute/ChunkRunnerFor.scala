package loamstream.model.execute

import loamstream.model.jobs.LJob

/**
 * @author clint
 * Nov 28, 2016
 */
abstract class ChunkRunnerFor(executionEnvironment: ExecutionEnvironment) extends ChunkRunner {
  override def canRun(job: LJob): Boolean = job.executionEnvironment == executionEnvironment
}