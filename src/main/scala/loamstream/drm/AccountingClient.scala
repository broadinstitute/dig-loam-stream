package loamstream.drm

import scala.concurrent.duration._

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getExecutionNode(jobId: String): Option[String]

  def getQueue(jobId: String): Option[Queue]
}

object AccountingClient {
  protected[drm] val defaultDelayStart: Duration = 0.5.seconds
  protected[drm] val defaultDelayCap: Duration = 30.seconds
  
  protected[drm] def delaySequence(start: Duration, cap: Duration): Iterator[Duration] = {
    require(start gt 0.seconds)
    require(cap gt 0.seconds)
    
    Iterator.iterate(start)(_ * 2).map(_.min(cap))
  }
}
