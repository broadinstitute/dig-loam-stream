package loamstream.drm

import scala.util.Try
import loamstream.model.execute.Resources.DrmResources
import loamstream.util.Tries
import loamstream.model.jobs.TerminationReason
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * May 1, 2019
 */
object MockAccountingClient {
  object NeverWorks extends AccountingClient {
    override def getResourceUsage(taskId: DrmTaskId): Future[DrmResources] = Future.fromTry(Tries.failure("MOCK"))
    
    override def getTerminationReason(taskId: DrmTaskId): Future[Option[TerminationReason]] = {
      Future.fromTry(Tries.failure("MOCK"))
    }
    
    override def getAccountingInfo(taskArray: DrmTaskArray): Future[Map[DrmTaskId, AccountingInfo]] = {
      Future.fromTry(Tries.failure("MOCK"))
    }
  }
}
