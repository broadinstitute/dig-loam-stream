package loamstream.model.execute

import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob
import loamstream.util.Shot
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.util.Hit
import scala.concurrent.Await

/**
 * @author clint
 * date: Jun 1, 2016
 */
final class LeavesFirstExecuter(implicit executionContext: ExecutionContext) extends LExecuter {
  import LJob.Result
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob,Shot[Result]] = {
    val futureAllResults = Future.sequence(executable.jobs.map(executeTree))
    
    val future = for {
      allResults <- futureAllResults
    } yield {
      allResults.foldLeft(Map.empty[LJob,Shot[Result]])(_ ++ _)
    }
    
    Await.result(future, timeout)
  }
  
  private def executeTree(job: LJob): Future[Map[LJob, Shot[Result]]] = {
    def loop(remainingOption: Option[LJob], acc: Map[LJob, Shot[Result]]): Future[Map[LJob, Shot[Result]]] = remainingOption match {
      case None => Future.successful(acc)
      /*case Some(j) if j.isLeaf => {
        j.execute.map(result => Map(j -> result))
      }*/
      case Some(j) => {
        val leaves: Set[LJob] = j.leaves
    
        val leavesToFutures: Set[Future[(LJob, Shot[Result])]] = leaves.map(leaf => leaf.execute.map(result => leaf -> Hit(result)))
    
        val futureLeafResultMap: Future[Map[LJob, Shot[Result]]] = Future.sequence(leavesToFutures).map(_.toMap)
        
        for {
          leafResults <- futureLeafResultMap
          next = if(j.isLeaf) None else Some(j.removeAll(leaves))  
          resultsSoFar <- loop(next, acc ++ leafResults)
        } yield resultsSoFar
      }
    }
   
    loop(Option(job), Map.empty)
  }
}