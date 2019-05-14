package loamstream.drm

import scala.util.Try
import loamstream.model.execute.Resources.DrmResources
import loamstream.util.Tries

/**
 * @author clint
 * May 1, 2019
 */
object MockAccountingClient {
  object NeverWorks extends AccountingClient {
    override def getResourceUsage(jobId: String): Try[DrmResources] = Tries.failure("MOCK")
  }
}
