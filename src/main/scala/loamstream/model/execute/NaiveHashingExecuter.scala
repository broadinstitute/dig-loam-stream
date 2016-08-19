package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob.Result
import loamstream.util.Hash
import loamstream.util.Maps
import loamstream.util.Shot
import loamstream.util.ValueBox
import loamstream.db.LoamDao
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.Output.CachedOutput
import scala.annotation.migration
import loamstream.util.TimeEnrichments
import loamstream.util.Traversables
import java.nio.file.Path
import loamstream.util.Hashes
import loamstream.util.PathUtils
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.Loggable

/**
 * @author clint
 * date: Aug 4, 2016
 * 
 * NB: This class contains a naive, first-pass sketch of an executor that takes job outputs' hashes into account.
 * It doesn't make use of any stored information (yet) and is for illustrative purposes only.
 */
final class NaiveHashingExecuter(dao: LoamDao)(implicit context: ExecutionContext) extends LExecuter with Loggable {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.mapValues(Shot(_))
    
    val futureResults = runAndMerge(executable.jobs).map(toShotMap)
    
    //TODO
    Await.result(futureResults, timeout)
  }
  
  //Support outputs other than Paths
  private[this] lazy val outputs: ValueBox[Map[Path, CachedOutput]] = {
    //TODO: All of them?  
    val map: Map[Path, CachedOutput] = dao.allRows.map(row => row.path -> row).toMap
    
    if(isDebugEnabled) {
      debug(s"Known paths: ${map.size}")
    
      map.values.foreach { data =>
        debug(data.toString)
      }
    }
    
    ValueBox(map)
  }
  
  private def normalize(p: Path) = p.toAbsolutePath
  
  private def isHashed(output: Path): Boolean = {
    outputs.value.contains(normalize(output))
  }
  
  private def notHashed(output: Path): Boolean = !isHashed(output)
      
  private def hasDifferentHash(output: Path): Boolean = {
    //TODO: Other hash types
    def hash(p: Path) = PathOutput(p).hash
    
    val path = normalize(output)
    
    outputs.value.get(path) match {
      case Some(cachedOutput) => cachedOutput.hash != hash(path) 
      case None => true
    }
  }
  
  private def isOlder(output: Path): Boolean = {
    import TimeEnrichments._
    
    def lastModified(p: Path) = PathOutput(p).lastModified
    
    val path = normalize(output)
    
    outputs.value.get(path) match {
      case Some(cachedOutput) => lastModified(path) < cachedOutput.lastModified
      case None => false
    }
  }
  
  private def shouldRun(dep: LJob): Boolean = {
    
    def needsToBeRun(output: Output): Boolean = output match {
      case Output.PathBased(p) => {
        val path = normalize(p)

        output.isMissing || isOlder(path) || notHashed(path) || hasDifferentHash(path)
      }
      case _ => true
    }
    
    val result = dep.outputs.isEmpty || dep.outputs.exists(needsToBeRun)
    
    if(!result) {
      debug("Skipping job $dep")
    }
    
    result
  }
  
  private def runWithoutDeps(job: LJob): Future[Result] = {
    val f = job.execute
    
    def cachedOutput(path: Path): CachedOutput = PathOutput(path).toCachedOutput
    
    import Traversables.Implicits._

    //NB: Use map here instead of foreach to ensure that side-effects happen before the resulting
    //future is done. 
    for {
      result <- f
    } yield {
      val outputPaths = job.outputs.collect { case Output.PathBased(path) => path }
        
      val newOutputs = outputPaths.mapTo(cachedOutput)
      
      outputs.mutate { oldOutputs =>
        oldOutputs ++ newOutputs 
      }
      
      dao.insertOrUpdate(newOutputs.values)
      
      result
    }
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