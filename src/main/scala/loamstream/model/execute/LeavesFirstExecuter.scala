package loamstream.model.execute

import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.Shot
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.util.Hit
import scala.concurrent.Await
import loamstream.util.Maps

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class LeavesFirstExecuter(implicit executionContext: ExecutionContext) extends LExecuter {
  
  import ExecuterHelpers._
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    val futureResults = Future.sequence(executable.jobs.map(executeJob)).map(Maps.mergeMaps)
    
    val future = futureResults.map(toShotMap)

    Await.result(future, timeout)
  }

  def executeLeaves(leaves: Set[LJob]): Future[Map[LJob, Result]] = {

    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val leafResultFutures = leaves.iterator.map(executeSingle)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
    //input jobs are evaluated before jobs that depend on them.
    val futureLeafResults = Future.sequence(leafResultFutures).map(consumeUntilFirstFailure)

    val futureAllLeafResultsMap = futureLeafResults.map(Maps.mergeMaps)
    
    futureAllLeafResultsMap
  }
  
  def executeJob(job: LJob)(implicit executionContext: ExecutionContext): Future[Map[LJob, Result]] = {
    def loop(remainingOption: Option[LJob], acc: Map[LJob, Result]): Future[Map[LJob, Result]] = {
      remainingOption match {
        case None => Future.successful(acc)
        case Some(j) => {
          val leaves = j.leaves
          
          for {
            leafResults <- executeLeaves(leaves)
            shouldStop = j.isLeaf //|| anyFailures(leafResults)
            next = if (shouldStop) None else Some(j.removeAll(leaves))
            resultsSoFar <- loop(next, acc ++ leafResults)
          } yield resultsSoFar
        }
      }
    }

    loop(Option(job), Map.empty)
  }
  
  private def anyFailures(m: Map[LJob, LJob.Result]): Boolean = m.values.exists(_.isFailure) 
}

object LeavesFirstExecuter {
  def default: LeavesFirstExecuter = new LeavesFirstExecuter()(scala.concurrent.ExecutionContext.global)
}