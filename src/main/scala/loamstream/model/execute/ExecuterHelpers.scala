package loamstream.model.execute

import java.util.concurrent.{Executors, ThreadFactory}

import scala.annotation.tailrec
import scala.concurrent.{ ExecutionContext, Future }

import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.Hit
import loamstream.util.Shot
import loamstream.util.Maps

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
  
  def toShotMap(m: Map[LJob, JobState]): Map[LJob, Shot[JobState]] = {
    import Maps.Implicits._
    
    m.strictMapValues(Hit(_))
  }

  /**
   * Creates and returns a fixed-size pool of daemon threads that will shutdown when non-daemon
   * threads complete so JVM is not prevented from exiting.
   * @param numThreads size of thread pool
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  def threadPool(numThreads: Int): ExecutionContext = {
    val es = Executors.newFixedThreadPool(numThreads, factoryWithDaemonThreads)
    ExecutionContext.fromExecutorService(es)
  }

  /**
   * @return Factory of daemon threads
   */
  def factoryWithDaemonThreads: ThreadFactory = new ThreadFactory() {
    val defaultFactory = Executors.defaultThreadFactory()

    override def newThread(r: Runnable): Thread = {
      val thread = defaultFactory.newThread(r)
      thread.setDaemon(true)
      thread
    }
  }
}
