package loamstream.drm

import org.scalatest.FunSuite

/**
  * @author clint
  * @date Aug 6, 2021
  *
  */
final class AccountingClientTest extends FunSuite {
  test("AlwaysFailsAccountingClient") {
    for {
      drmSystem <- DrmSystem.values
    } {
      val client = new AccountingClient.AlwaysFailsAccountingClient(drmSystem)
      
      import client._    
      import monix.execution.Scheduler.Implicits.global

      val taskId = DrmTaskId("foo", 123)

      assert(getTerminationReason(taskId).runSyncUnsafe() === None)
      
      intercept[Exception] {
        getResourceUsage(taskId).runSyncUnsafe()
      }

      intercept[Exception] {
        getAccountingInfo(taskId).runSyncUnsafe()
      }
    }
  }
}
