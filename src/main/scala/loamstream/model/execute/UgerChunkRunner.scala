package loamstream.model.execute

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.Maps

final class UgerChunkRunner extends ChunkRunner {
  import ExecuterHelpers._
  
  override def run(leaves: Set[LJob])(implicit context: ExecutionContext): Future[Map[LJob, Result]] = {
    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val leafResultFutures = leaves.iterator.map(executeSingle)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
    //input jobs are evaluated before jobs that depend on them.
    val futureLeafResults = Future.sequence(leafResultFutures).map(consumeUntilFirstFailure)

    futureLeafResults.map(Maps.mergeMaps)
  }
}