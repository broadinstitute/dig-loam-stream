package loamstream.uger

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller {
  /**
   * 
   */
  def poll(jobId: String, timeout: Duration): Future[Try[JobStatus]]
}

object Poller {
  
  final class DrmaaPoller(client: DrmaaClient)(implicit context: ExecutionContext) extends Poller {
    override def poll(jobId: String, timeout: Duration): Future[Try[JobStatus]] = Future {
      blocking {
        client.waitFor(jobId, timeout)
      }
    }
  }
  
  def drmaa(client: DrmaaClient)(implicit context: ExecutionContext): Poller = new DrmaaPoller(client)
}