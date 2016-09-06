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
final class NaiveFilteringExecuter(jobFilter: JobFilter)
    (implicit context: ExecutionContext) extends LExecuter with Loggable {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.mapValues(Shot(_))
    
    val futureResults = runAndMerge(executable.jobs).map(toShotMap)
    
    //TODO
    Await.result(futureResults, timeout)
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
      jobFilter.record(job.outputs)
      
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
    val toBeRun = jobs.iterator.filter(jobFilter.shouldRun)
    
    val rawDepResults = toBeRun.map(run)
    
    Future.sequence(rawDepResults).map(Maps.mergeMaps)
  }
}