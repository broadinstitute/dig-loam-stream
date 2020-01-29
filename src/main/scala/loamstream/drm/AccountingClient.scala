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
  def getResourceUsage(taskId: DrmTaskId): Future[DrmResources]
  
  def getTerminationReason(taskId: DrmTaskId): Future[Option[TerminationReason]]
  
  def getAccountingInfo(taskId: DrmTaskId)(implicit ex: ExecutionContext): Future[AccountingInfo] = {
    for {
      rs <- getResourceUsage(taskId)
      tr <- getTerminationReason(taskId)
    } yield AccountingInfo(rs, tr)
  }
}
