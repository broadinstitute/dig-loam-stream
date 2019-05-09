package loamstream.drm

import scala.concurrent.duration._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import loamstream.util.Loggable
import loamstream.util.Functions
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.util.Tries
import loamstream.util.Loops
import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getResourceUsage(jobId: String): Try[DrmResources]
}
