package loamstream.drm

import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import monix.eval.Task


/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getResourceUsage(taskId: DrmTaskId): Task[DrmResources]
  
  def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]]
  
  def getAccountingInfo(taskId: DrmTaskId): Task[AccountingInfo] = {
    val rsf = getResourceUsage(taskId)
    val trf = getTerminationReason(taskId)
    
    Task.parMap2(rsf, trf)(AccountingInfo(_, _))
  }
}
