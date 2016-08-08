package loamstream.model.jobs

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.model.execute.LExecutable

import loamstream.model.execute.LExecuter
import loamstream.model.jobs.LJob.Result
import loamstream.util.Hash
import loamstream.util.Maps
import loamstream.util.Shot
import loamstream.util.ValueBox
import loamstream.db.LoamDao

/**
 * @author clint
 * date: Aug 4, 2016
 * 
 * NB: This class contains a naive, first-pass sketch of an executor that takes job outputs' hashes into account.
 * It doesn't make use of any stored information (yet) and is for illustrative purposes only.
 */
final class NaiveHashingExecuter(dao: LoamDao)(implicit context: ExecutionContext) extends LExecuter {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.mapValues(Shot(_))
    
    val futureResults = runAndMerge(executable.jobs).map(toShotMap)
    
    //TODO
    Await.result(futureResults, timeout)
  }
  
  private[this] val hashes: ValueBox[Map[Output, Hash]] = {
    //TODO: 
    
    ValueBox(Map.empty)
  }
  
  private def isHashed(output: Output): Boolean = hashes.value.contains(output)
      
  private def hasSameHash(output: Output): Boolean = hashes.value.get(output).map(_ == output.hash).getOrElse(false)
  
  private def shouldRun(dep: LJob): Boolean = {
    dep.outputs.exists { output =>
      !isHashed(output) || !hasSameHash(output)
    }
  }
  
  private def runWithoutDeps(job: LJob): Future[Result] = {
    val f = job.execute
    
    f.foreach { _ =>
      hashes.mutate { oldHashes =>
        val newHashes = (job.outputs.map(o => o -> o.hash))
        
        oldHashes ++ newHashes 
      }
    }
    
    f
  }
  
  private def run(job: LJob): Future[Map[LJob, Result]] = {
    val futureDepResults = runAndMerge(job.inputs)
    
    for {
      depResults <- futureDepResults
      jobResult <- runWithoutDeps(job)
    } yield {
      depResults + (job -> jobResult)
    }
  }
  
  private def runAndMerge(jobs: Iterable[LJob]): Future[Map[LJob, Result]] = {
    val toBeRun = jobs.iterator.filter(shouldRun)
    
    val rawDepResults = toBeRun.map(run)
    
    Future.sequence(rawDepResults).map(Maps.mergeMaps)
  }
}