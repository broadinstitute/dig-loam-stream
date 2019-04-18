package loamstream.drm

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
  private[drm] def delaySequence: Iterator[Duration] = {
    Iterator.iterate(0.5)(_ * 2).map(scala.math.min(30, _)).map(_.seconds)
  }
}
