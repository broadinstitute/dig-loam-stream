package loamstream.uger

import org.scalatest.FunSuite
import org.ggf.drmaa.Session
import loamstream.model.jobs.JobState

/**
 * @author clint
 * Jan 5, 2017
 */
final class UgerStatusTest extends FunSuite {
  import UgerStatus._

  test("fromUgerStatusCode") {
    import Session._
    
    assert(fromUgerStatusCode(QUEUED_ACTIVE) === Queued)
    assert(fromUgerStatusCode(SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(RUNNING) === Running)
    assert(fromUgerStatusCode(SYSTEM_SUSPENDED) === Suspended)
    assert(fromUgerStatusCode(USER_SUSPENDED) === Suspended)
    assert(fromUgerStatusCode(USER_SYSTEM_SUSPENDED) === Suspended)
    assert(fromUgerStatusCode(DONE) === Done)
    assert(fromUgerStatusCode(FAILED) === Failed)
    assert(fromUgerStatusCode(UNDETERMINED) === Undetermined)
    assert(fromUgerStatusCode(-123456) === Undetermined)
    assert(fromUgerStatusCode(123456) === Undetermined)
    assert(fromUgerStatusCode(Int.MinValue) === Undetermined)
    assert(fromUgerStatusCode(Int.MaxValue) === Undetermined)
  }

  test("toJobState") {
    assert(toJobState(Done) === JobState.Succeeded)
    
    assert(toJobState(CommandResult(-1)) === JobState.CommandResult(-1))
    assert(toJobState(CommandResult(0)) === JobState.CommandResult(0))
    assert(toJobState(CommandResult(42)) === JobState.CommandResult(42))
    
    assert(toJobState(DoneUndetermined) === JobState.Failed)
    assert(toJobState(Failed) === JobState.Failed)
    
    assert(toJobState(Queued) === JobState.Running)
    assert(toJobState(QueuedHeld) === JobState.Running)
    assert(toJobState(Requeued) === JobState.Running)
    assert(toJobState(RequeuedHeld) === JobState.Running)
    assert(toJobState(Running) === JobState.Running)
    
    assert(toJobState(Suspended) === JobState.Failed)
    assert(toJobState(Undetermined) === JobState.Failed)
  }
  
  
  test("isDone") {
    doFlagTest(
      _.isDone, 
      expectedTrueFor = Done, 
      expectedFalseFor = DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined)
  }
  
  test("isFailed") {
    doFlagTest(
      _.isFailed, 
      expectedTrueFor = Failed, 
      expectedFalseFor = Done, DoneUndetermined, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined)
  }
  
  test("isQueued") {
    doFlagTest(
      _.isQueued, 
      expectedTrueFor = Queued, 
      expectedFalseFor = Done, DoneUndetermined, Failed, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined)
  }
  
  test("isQueuedHeld") {
    doFlagTest(
      _.isQueuedHeld, 
      expectedTrueFor = QueuedHeld, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined)
  }
  
  test("isRunning") {
    doFlagTest(
      _.isRunning, 
      expectedTrueFor = Running, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Suspended, Undetermined)
  }
  
  test("isSuspended") {
    doFlagTest(
      _.isSuspended, 
      expectedTrueFor = Suspended, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Undetermined)
  }
  
  test("isUndetermined") {
    doFlagTest(
      _.isUndetermined, 
      expectedTrueFor = Undetermined, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended)
  }
  
  test("isDoneUndetermined") {
    doFlagTest(
      _.isDoneUndetermined, 
      expectedTrueFor = DoneUndetermined, 
      expectedFalseFor = Done, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined)
  }

  test("notFinished") {
    assert(Queued.notFinished === true)
    assert(QueuedHeld.notFinished === true)
    assert(Running.notFinished === true)
    assert(Suspended.notFinished === true)
    assert(Undetermined.notFinished === true)
    
    assert(Done.notFinished === false)
    assert(DoneUndetermined.notFinished === false)
    assert(Failed.notFinished === false)
    //TODO: ???
    assert(Requeued.notFinished === false)
    //TODO: ???
    assert(RequeuedHeld.notFinished === false)
  }
  
  test("isFinished") {
    assert(Queued.isFinished === false)
    assert(QueuedHeld.isFinished === false)
    assert(Running.isFinished === false)
    assert(Suspended.isFinished === false)
    assert(Undetermined.isFinished === false)
    
    assert(Done.isFinished === true)
    assert(DoneUndetermined.isFinished === true)
    assert(Failed.isFinished === true)
    //TODO: ???
    assert(Requeued.isFinished === true)
    //TODO: ???
    assert(RequeuedHeld.isFinished === true)
  }
  
  private def doFlagTest(flag: UgerStatus => Boolean, expectedTrueFor: UgerStatus, expectedFalseFor: UgerStatus*): Unit = {
    assert(flag(expectedTrueFor) === true)
    
    for(ugerStatus <- expectedFalseFor) {
      assert(flag(ugerStatus) === false)
    }
  }
}
