package loamstream.model.execute


import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }

import loamstream.model.jobs.JobResult
import loamstream.model.jobs.LJob

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers {
  def consumeUntilFirstFailure(iter: Iterator[Map[LJob, JobResult]]): IndexedSeq[Map[LJob, JobResult]] = {
    @tailrec
    def loop(acc: IndexedSeq[Map[LJob, JobResult]]): IndexedSeq[Map[LJob, JobResult]] = {
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

  def noFailures[J <: LJob](m: Map[J, JobResult]): Boolean = m.values.forall(_.isSuccess)
  
  def anyFailures[J <: LJob](m: Map[J, JobResult]): Boolean = !noFailures(m)

  def executeSingle(job: LJob)(implicit executor: ExecutionContext): Future[Map[LJob, JobResult]] = {
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
