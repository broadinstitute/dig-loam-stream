package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.InvalidJobException
import loamstream.util.Classes.simpleNameOf
import loamstream.util.Loggable
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jun 21, 2016
 */
final class DrmaaPoller(client: DrmaaClient) extends Poller with Loggable {
  
  override def stop(): Unit = client.stop()
  
  override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    
    def statusAttempt(taskId: DrmTaskId): Try[DrmStatus] = {
      val result = client.statusOf(taskId).recoverWith { case e: InvalidJobException =>
        debug(s"Job '$taskId': Got an ${simpleNameOf(e)} when calling statusOf(); trying waitFor()", e)
      
        client.waitFor(taskId, Duration.Zero)
      }
      
      //Ignore the result of recover, we just want the logging side-effect
      result.recover {
        case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
      }
      
      result
    }
    
    //NB: Sort job ids for better log output
    lazy val sortedJobIds = jobIds.toSeq.sorted
    
    trace(s"Polling status of jobs ${sortedJobIds.mkString(",")}")
    
    val pollResults = Observable.from(jobIds).map { jobId =>
      jobId -> statusAttempt(jobId)
    }
    
    trace(s"Polled ${sortedJobIds.mkString(",")}")
    
    pollResults
  }
}
