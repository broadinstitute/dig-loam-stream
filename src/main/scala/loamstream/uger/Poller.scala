package loamstream.uger

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Poller {
  def poll(jobId: String): Future[JobStatus]
}

object Poller {
  
  final class DrmaaPoller(client: DrmaaClient)(implicit context: ExecutionContext) extends Poller {
    override def poll(jobId: String): Future[JobStatus] = Future {
      //TODO: Something better
      client.statusOf(jobId).get
    }
  }
  
  def drmaa1(client: DrmaaClient)(implicit context: ExecutionContext): Poller = new DrmaaPoller(client)
}