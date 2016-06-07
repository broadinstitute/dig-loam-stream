package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.{ Hit, Shot }
import loamstream.util.Loggable
import loamstream.util.Maps

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
final class SimpleExecuter(implicit executionContext: ExecutionContext) extends LExecuter with Loggable {

  import ExecuterHelpers._
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    val future = executeJobs(executable.jobs).map(toShotMap)

    Await.result(future, timeout)
  }
  
  private def executeJobs(jobs: Iterable[LJob]): Future[Map[LJob, Result]] = {
    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val subResultFutures = jobs.iterator.map(executeJob)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
    //input jobs are evaluated before jobs that depend on them.
    val futureSubResults = Future.sequence(subResultFutures).map(consumeUntilFirstFailure)

    futureSubResults.map(Maps.mergeMaps)
  }
  
  private def executeJob(job: LJob): Future[Map[LJob, Result]] = {
    for {
      inputResults <- executeJobs(job.inputs)
      resultMapping <- executeSingle(job)
    } yield {
      inputResults ++ resultMapping
    }
  }
}
