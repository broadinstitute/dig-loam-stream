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
