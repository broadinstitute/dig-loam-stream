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
  final class Drmaa2Poller(client: Drmaa2Client)(implicit context: ExecutionContext) extends Poller {
    override def poll(jobId: String): Future[JobStatus] = Future {
      //TODO: Something better
      client.statusOf(jobId).get 
    }
  }
  
  def drmaa2(client: Drmaa2Client)(implicit context: ExecutionContext): Poller = new Drmaa2Poller(client)
}