package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.InvalidJobException
import loamstream.util.Classes.simpleNameOf
import loamstream.util.Loggable

/**
 * @author clint
 * date: Jun 21, 2016
 */
final class DrmaaPoller(client: DrmaaClient) extends Poller with Loggable {
  
  override def stop(): Unit = client.stop()
  
  override def poll(jobIds: Iterable[String]): Map[String, Try[DrmStatus]] = {
    
    def statusAttempt(jobId: String): Try[DrmStatus] = {
      val result = client.statusOf(jobId).recoverWith { case e: InvalidJobException =>
        debug(s"Job '$jobId': Got an ${simpleNameOf(e)} when calling statusOf(); trying waitFor()", e)
      
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
