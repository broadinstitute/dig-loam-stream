package loamstream.uger

import scala.concurrent.duration.Duration
import scala.util.Try

import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.InvalidJobException

import loamstream.util.Loggable

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller {
  /**
   * Synchronously inquire about the status of one or more jobs
 *
   * @param jobIds the ids of the jobs to inquire about
   * @return a map of job ids to attempts at that job's status
   */
  def poll(jobIds: Iterable[String]): Map[String, Try[UgerStatus]]
}

object Poller {
  
  final class DrmaaPoller(client: DrmaaClient) extends Poller with Loggable {
    override def poll(jobIds: Iterable[String]): Map[String, Try[UgerStatus]] = {
      def statusAttempt(jobId: String): Try[UgerStatus] = {
        
        val result = client.statusOf(jobId).recoverWith { case e: InvalidJobException => 
          debug(s"Job '$jobId': Got an ${e.getClass.getSimpleName} when calling statusOf(); trying waitFor()", e)
          
          client.waitFor(jobId, Duration.Zero)
        }
        
        //Ignore the result of recover, we just want the logging side-effect
        result.recover {
          case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
        }
        
        result
      }
      
      //NB: Sort job ids for better log output
      val sortedJobIds = jobIds.toSeq.sorted
      
      debug(s"Polling status of jobs ${sortedJobIds.mkString(",")}")
      
      val pollResults = sortedJobIds.map { jobId =>
        jobId -> statusAttempt(jobId)
      }
      
      debug(s"Polled ${sortedJobIds.mkString(",")}")
      
      pollResults.toMap
    }
  }
  
  def drmaa(client: DrmaaClient): Poller = new DrmaaPoller(client)
}
