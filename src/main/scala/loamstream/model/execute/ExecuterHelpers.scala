package loamstream.model.execute

import java.util.concurrent.{Executors, ThreadFactory}

import scala.annotation.tailrec
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
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

  /**
   * Creates and returns a fixed-size pool of daemon threads that will shutdown when non-daemon
   * threads complete so JVM is not prevented from exiting.
   * @param numThreads size of thread pool
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  def threadPool(numThreads: Int): ExecutionContext = {
    val es = Executors.newFixedThreadPool(numThreads, factoryWithDaemonThreads())
    ExecutionContext.fromExecutorService(es)
  }

  /**
   * @return Factory of daemon threads
   */
  def factoryWithDaemonThreads() = new ThreadFactory() {
    val defaultFactory = Executors.defaultThreadFactory()

    override def newThread(r: Runnable): Thread = {
      val thread = defaultFactory.newThread(r)
      thread.setDaemon(true)
      thread
    }
  }
}
