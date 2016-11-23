package loamstream.model.execute

import java.util.concurrent.{Executors, ThreadFactory}

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }

import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.util.Maps
import loamstream.util.ExecutorServices
import loamstream.util.Terminable

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers {
  def consumeUntilFirstFailure(iter: Iterator[Map[LJob, JobState]]): IndexedSeq[Map[LJob, JobState]] = {
    @tailrec
    def loop(acc: IndexedSeq[Map[LJob, JobState]]): IndexedSeq[Map[LJob, JobState]] = {
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

  def noFailures[J <: LJob](m: Map[J, JobState]): Boolean = m.values.forall(_.isSuccess)

  def executeSingle(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, JobState]] = {
    for {
      result <- job.execute
    } yield {
      Map(job -> result)
    }
  }
  
  def flattenTree(roots: Set[LJob]): Set[LJob] = {
    roots.foldLeft(roots) { (acc, job) =>
      job.inputs ++ flattenTree(job.inputs) ++ acc
    }
  }
}
