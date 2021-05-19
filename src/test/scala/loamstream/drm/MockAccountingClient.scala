package loamstream.drm

import loamstream.model.execute.Resources.DrmResources
import loamstream.model.jobs.TerminationReason
import loamstream.util.Tries
import monix.eval.Task

/**
 * @author clint
 * May 1, 2019
 */
object MockAccountingClient {
  object NeverWorks extends AccountingClient {
    override def getResourceUsage(taskId: DrmTaskId): Task[DrmResources] = Task.fromTry(Tries.failure("MOCK"))
    
    override def getTerminationReason(taskId: DrmTaskId): Task[Option[TerminationReason]] = {
      Task.fromTry(Tries.failure("MOCK"))
    }
  }
}
