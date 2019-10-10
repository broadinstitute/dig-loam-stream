package loamstream.drm

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getResourceUsage(jobId: String): Future[DrmResources]
  
  def getTerminationReason(jobId: String): Future[Option[TerminationReason]]
  
  def getAccountingInfo(jobId: String)(implicit ex: ExecutionContext): Future[AccountingInfo] = {
    for {
      rs <- getResourceUsage(jobId)
      tr <- getTerminationReason(jobId)
    } yield AccountingInfo(rs, tr)
  }
}
