package loamstream.drm

import loamstream.model.jobs.TerminationReason
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * May 8, 2019
 */
final case class AccountingInfo(resources: DrmResources, terminationReasonOpt: Option[TerminationReason])

object AccountingInfo {
  def fromResources(resources: DrmResources): AccountingInfo = AccountingInfo(resources, terminationReasonOpt = None)
}
