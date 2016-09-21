package loamstream.uger

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import loamstream.util.TimeEnrichments._
import scala.util.Try
import rx.lang.scala.Observable
import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import scala.util.Success
import rx.lang.scala.subjects.ReplaySubject
import org.ggf.drmaa.InvalidJobException
import loamstream.util.Maps

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller {
  /**
   * Synchronously inquire about the status of one or more jobs
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  def poll(jobIds: Iterable[String]): Map[String, Try[JobStatus]]
}

object Poller {
  
  final class DrmaaPoller(client: DrmaaClient)(implicit context: ExecutionContext) extends Poller {
    override def poll(jobIds: Iterable[String]): Map[String, Try[JobStatus]] = {
      
      def statusAttempt(jobId: String): Try[JobStatus] = {
        client.statusOf(jobId).recoverWith {
          case e: InvalidJobException => client.waitFor(jobId, Duration.Zero)
        }
      }
      
      val pollResults = jobIds.map { jobId =>
        jobId -> statusAttempt(jobId)
      }
      
      pollResults.toMap
    }
  }
  
  def drmaa(client: DrmaaClient)(implicit context: ExecutionContext): Poller = new DrmaaPoller(client)
}