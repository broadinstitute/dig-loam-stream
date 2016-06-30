package loamstream.uger

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.concurrent.duration.Duration

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller {
  def poll(jobId: String, timeout: Duration): Future[Try[JobStatus]]
}

object Poller {
  
  final class DrmaaPoller(client: DrmaaClient)(implicit context: ExecutionContext) extends Poller {
    override def poll(jobId: String, timeout: Duration): Future[Try[JobStatus]] = Future {
      client.waitFor(jobId, timeout)
    }
  }
  
  def drmaa1(client: DrmaaClient)(implicit context: ExecutionContext): Poller = new DrmaaPoller(client)
}