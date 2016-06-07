package loamstream.model.execute

import scala.annotation.tailrec
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.util.Maps

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers {
  def consumeUntilFirstFailure(iter: Iterator[Map[LJob, Result]]): IndexedSeq[Map[LJob, Result]] = {
    @tailrec
    def loop(acc: IndexedSeq[Map[LJob, Result]]): IndexedSeq[Map[LJob, Result]] = {
      if (iter.isEmpty) { acc }
      else {
        val m = iter.next()

        val shouldKeepGoing = noFailures(m)

        val newAcc = acc :+ m

        if (shouldKeepGoing) { loop(newAcc) }
        else { newAcc }
      }
    }

    loop(Vector.empty)
  }

  def noFailures[J <: LJob](m: Map[J, Result]): Boolean = m.values.forall(_.isSuccess)

  def executeSingle(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, Result]] = {
    for {
      result <- job.execute
    } yield {
      Map(job -> result)
    }
  }
  
  def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = {
    import Maps.Implicits._
    
    m.strictMapValues(Hit(_))
  }
}