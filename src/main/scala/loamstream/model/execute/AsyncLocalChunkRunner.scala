package loamstream.model.execute

import scala.concurrent.ExecutionContext
import loamstream.model.jobs.LJob
import scala.concurrent.Future
import loamstream.model.jobs.JobState
import loamstream.util.Maps

import AsyncLocalChunkRunner.defaultMaxNumJobs

/**
 * @author clint
 * Nov 22, 2016
 */
final case class AsyncLocalChunkRunner(
    maxNumJobs: Int = defaultMaxNumJobs) extends ChunkRunnerFor(ExecutionEnvironment.Local) {

  import ExecuterHelpers._

  override def run(jobs: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, JobState]] = {
    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val jobResultFutures = jobs.iterator.map(executeSingle)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure
    //input jobs are evaluated before jobs that depend on them.
    val futureJobResults = Future.sequence(jobResultFutures).map(consumeUntilFirstFailure)

    futureJobResults.map(Maps.mergeMaps)
  }
}

object AsyncLocalChunkRunner {
  def defaultMaxNumJobs: Int = Runtime.getRuntime.availableProcessors
}