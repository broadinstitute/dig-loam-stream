package loamstream.uger

import loamstream.util.Loggable

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}
import org.ggf.drmaa.{DrmaaException, InvalidJobException}

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
  
  final class DrmaaPoller(client: DrmaaClient) extends Poller with Loggable {
    override def poll(jobIds: Iterable[String]): Map[String, Try[JobStatus]] = {
      
      def statusAttempt(jobId: String): Try[JobStatus] = {
        val result = client.statusOf(jobId).recoverWith {
          case e: InvalidJobException => client.waitFor(jobId, Duration.Zero)
        }
        
        //Ignore the result of recover, we just want the logging side-effect
        result.recover {
          case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
        }
        
        result
      }
      
      val pollResults = jobIds.map { jobId =>
        jobId -> statusAttempt(jobId)
      }
      
      pollResults.toMap
    }
  }
  
  def drmaa(client: DrmaaClient)(implicit context: ExecutionContext): Poller = new DrmaaPoller(client)
}
