package loamstream.apps.minimal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration



import loamstream.model.execute.{ LExecutable, LExecuter }
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.Maps
import loamstream.util.shot.{ Hit, Shot }

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
object MiniExecuter extends LExecuter {

  override def execute(executable: LExecutable): Map[LJob, Shot[Result]] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    import Maps.Implicits._
    
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.strictMapValues(Hit(_))
    
    val mergedResults = execJobs(executable.jobs).map(toShotMap)

    //TODO: Block here?
    Await.result(mergedResults, Duration.Inf)
  }

  private[minimal] def execJobs(jobs: Iterable[LJob])(implicit ctx: ExecutionContext): Future[Map[LJob, Result]] = {
    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val subResultFutures = jobs.iterator.map(exec)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
    //input jobs are evaluated before jobs that depend on them.
    val futureSubResults = Future.sequence(subResultFutures).map(_.takeWhile(noFailures).toIndexedSeq)

    futureSubResults.map(Maps.mergeMaps)
  }
  
  private[minimal] def exec(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, Result]] = {
    for {
      inputResults <- execJobs(job.inputs)
      resultMapping <- executeLeaf(job)
    } yield {
      inputResults ++ resultMapping 
    }
  }
  
  private[minimal] def noFailures[J <: LJob](m: Map[J, Result]): Boolean = {
    m.values.forall(_.isSuccess)
  }

  private[minimal] def executeLeaf(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, Result]] = {
    for {
      result <- job.execute
    } yield {
      if (result.isSuccess) { Map(job -> result) }
      else { Map.empty[LJob, Result] }
    }
  }
}
