package loamstream.model.jobs

import loamstream.model.execute.LExecuter
import loamstream.model.execute.LExecutable
import scala.concurrent.duration.Duration
import loamstream.util.Shot
import loamstream.model.jobs.LJob.Result
import loamstream.util.SyncRef
import scala.concurrent.Future
import loamstream.util.Futures
import loamstream.util.Maps
import scala.concurrent.ExecutionContext
import loamstream.util.Hit
import scala.concurrent.Await
import loamstream.util.Hash

/**
 * @author clint
 * date: Aug 4, 2016
 */
final class NewExecuter(implicit context: ExecutionContext) extends LExecuter {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    ???
  }
  
  def newExecute(jobs: Set[NewJob])(implicit timeout: Duration = Duration.Inf): Map[NewJob, Shot[Result]] = {
    def toShotMap(m: Map[NewJob, Result]): Map[NewJob, Shot[Result]] = m.mapValues(Shot(_))
    
    val futureResults = runAndMerge(jobs).map(toShotMap)
    
    //TODO
    Await.result(futureResults, timeout)
  }
  
  private def isHashed(output: Output): Boolean = hashes().contains(output)
      
  private def hasSameHash(output: Output): Boolean = hashes().get(output).map(_ == output.hash).getOrElse(false)
      
  private[this] val hashes: SyncRef[Map[Output, Hash]] = SyncRef(Map.empty)
  
  private def shouldRun(dep: NewJob): Boolean = {
    dep.outputs.exists { output =>
      !isHashed(output) || !hasSameHash(output)
    }
  }
  
  private def runWithoutDeps(job: NewJob): Future[Result] = {
    val f = job.execute
    
    f.foreach { _ =>
      hashes.mutate { oldHashes =>
        val newHashes = (job.outputs.map(o => o -> o.hash))
        
        oldHashes ++ newHashes 
      }
    }
    
    f
  }
  
  private def run(job: NewJob): Future[Map[NewJob, Result]] = {
    val futureDepResults = runAndMerge(job.inputs)
    
    for {
      depResults <- futureDepResults
      jobResult <- runWithoutDeps(job)
    } yield {
      depResults + (job -> jobResult)
    }
  }
  
  private def runAndMerge(jobs: Iterable[NewJob]): Future[Map[NewJob, Result]] = {
    val toBeRun = jobs.iterator.filter(shouldRun)
    
    val rawDepResults = toBeRun.map(run)
    
    Future.sequence(rawDepResults).map(Maps.mergeMaps)
  }
}