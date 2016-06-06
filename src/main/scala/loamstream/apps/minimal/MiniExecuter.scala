package loamstream.apps.minimal

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import loamstream.model.execute.{ LExecutable, LExecuter }
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.{ Hit, Shot }
import loamstream.util.Loggable
import loamstream.util.Maps

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
object MiniExecuter extends LExecuter with Loggable {

  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    import Maps.Implicits._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.strictMapValues(Hit(_))
    
    val mergedResults = execJobs(executable.jobs).map(toShotMap)

    //TODO: Block here?
    Await.result(mergedResults, timeout)
  }

  private[minimal] def execJobs(jobs: Iterable[LJob])(implicit ctx: ExecutionContext): Future[Map[LJob, Result]] = {
    //NB: Use an iterator to evaluate input jobs lazily, so we can stop evaluating
    //on the first failure, like the old code did.
    val subResultFutures = jobs.iterator.map(exec)

    //NB: Convert the iterator to an IndexedSeq to force evaluation, and make sure 
    //input jobs are evaluated before jobs that depend on them.
    val futureSubResults = Future.sequence(subResultFutures).map(consumeUntilFirstFailure)

    futureSubResults.map(Maps.mergeMaps)
  }
  
  private[minimal] def consumeUntilFirstFailure(iter: Iterator[Map[LJob, Result]]): IndexedSeq[Map[LJob, Result]] = {
    @tailrec
    def loop(acc: IndexedSeq[Map[LJob, Result]]): IndexedSeq[Map[LJob, Result]] = {
      if(iter.isEmpty) { acc }
      else {
        val m = iter.next()
      
        val shouldKeepGoing = noFailures(m)
        
        val newAcc = acc :+ m
        
        if(shouldKeepGoing) { loop(newAcc) }
        else { newAcc }
      }
    }
    
    loop(Vector.empty)
  }
  
  private[minimal] def exec(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, Result]] = {
    for {
      inputResults <- execJobs(job.inputs)
      resultMapping <- execSingle(job)
    } yield {
      inputResults ++ resultMapping
    }
  }
  
  private[minimal] def noFailures[J <: LJob](m: Map[J, Result]): Boolean = m.values.forall(_.isSuccess)

  private[minimal] def execSingle(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, Result]] = {
    for {
      result <- job.execute
    } yield {
      Map(job -> result)
    }
  }
}
