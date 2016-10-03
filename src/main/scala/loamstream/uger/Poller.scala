package loamstream.uger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try
import org.ggf.drmaa.InvalidJobException
import loamstream.util.Loggable

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
  
  final class DrmaaPoller(client: DrmaaClient)(implicit context: ExecutionContext) extends Poller with Loggable {
    override def poll(jobIds: Iterable[String]): Map[String, Try[JobStatus]] = {
      
      def statusAttempt(jobId: String): Try[JobStatus] = {
        client.statusOf(jobId).recoverWith { case e: InvalidJobException =>
          warn(s"Job '$jobId': Got an ${e.getClass.getSimpleName} when calling statusOf(); trying waitFor()", e)
          
          client.waitFor(jobId, Duration.Zero)
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
