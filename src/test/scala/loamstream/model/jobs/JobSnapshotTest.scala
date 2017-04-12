package loamstream.model.jobs

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 12, 2017
 */
final class JobSnapshotTest extends FunSuite {
  import JobStatus._
  
  test("transitionTo") {
    def doTest(startingFrom: JobStatus): Unit = {
      val snapshot = JobSnapshot(startingFrom, 42)
      
      def doTestTo(newStatus: JobStatus): Unit = { 
        assert(snapshot.transitionTo(newStatus) === JobSnapshot(newStatus, 42))
      }

      //Everything but 'Running' should leave runCount alone
      doTestTo(Succeeded)
      doTestTo(Skipped)
      doTestTo(Failed)
      doTestTo(FailedWithException)
      doTestTo(Terminated)
      doTestTo(NotStarted)
      doTestTo(Submitted)
      doTestTo(Unknown)
      doTestTo(FailedPermanently)
      
      //Going to running bumps runCount
      val running = snapshot.transitionTo(Running)
      
      assert(running === JobSnapshot(Running, 43))
      
      //Going to running when you're already running leaves runCount unchanged
      assert(running.transitionTo(Running) === JobSnapshot(Running, 43))

      //Going to anything else from running leaves runCount unchanged
      running.transitionTo(Failed) === running
    }
    
    doTest(Succeeded)
    doTest(Skipped)
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
    doTest(NotStarted)
    doTest(Submitted)
    doTest(Unknown)
    doTest(FailedPermanently)
  }
}
