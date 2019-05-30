package loamstream.drm

import scala.util.Try
import loamstream.model.execute.Resources.DrmResources
import loamstream.util.Tries
import loamstream.model.jobs.TerminationReason

/**
 * @author clint
 * May 1, 2019
 */
object MockAccountingClient {
  object NeverWorks extends AccountingClient {
    override def getResourceUsage(jobId: String): Try[DrmResources] = Tries.failure("MOCK")
    
    override def getTerminationReason(jobId: String): Try[Option[TerminationReason]] = Tries.failure("MOCK")
  }
}