package loamstream.uger

import org.scalatest.FunSuite
import org.ggf.drmaa.Session
import loamstream.model.jobs.JobState
import loamstream.model.execute.Memory
import java.time.Instant
import loamstream.model.execute.CpuTime
import loamstream.model.execute.Resources.UgerResources

/**
 * @author clint
 * Jan 5, 2017
 */
final class UgerStatusTest extends FunSuite {
  import UgerStatus._

  //scalastyle:off magic.number
  
  import scala.concurrent.duration._
    
  private val resources = UgerResources(
      Memory.inGb(2), 
      CpuTime(1.second), 
      Some("example.com"), 
      Some(Queue.Long),
      Instant.now,
      Instant.now)
  
  test("fromUgerStatusCode") {
    import Session._
    
    assert(fromUgerStatusCode(QUEUED_ACTIVE) === Queued)
    assert(fromUgerStatusCode(SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(USER_SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromUgerStatusCode(RUNNING) === Running)
    assert(fromUgerStatusCode(SYSTEM_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(USER_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(USER_SYSTEM_SUSPENDED) === Suspended())
    assert(fromUgerStatusCode(DONE) === Done)
    assert(fromUgerStatusCode(FAILED) === Failed())
    assert(fromUgerStatusCode(UNDETERMINED) === Undetermined())
    assert(fromUgerStatusCode(-123456) === Undetermined())
    assert(fromUgerStatusCode(123456) === Undetermined())
    assert(fromUgerStatusCode(Int.MinValue) === Undetermined())
    assert(fromUgerStatusCode(Int.MaxValue) === Undetermined())
  }

  test("toJobState") {
    assert(toJobState(Done) === JobState.Succeeded)
    
    assert(toJobState(CommandResult(-1, Some(resources))) === JobState.CommandResult(-1, Some(resources)))
    assert(toJobState(CommandResult(0, Some(resources))) === JobState.CommandResult(0, Some(resources)))
    assert(toJobState(CommandResult(42, Some(resources))) === JobState.CommandResult(42, Some(resources)))
    
    assert(toJobState(DoneUndetermined(Some(resources))) === JobState.Failed(Some(resources)))
    assert(toJobState(Failed()) === JobState.Failed())
    assert(toJobState(Failed(Some(resources))) === JobState.Failed(Some(resources)))
    
    assert(toJobState(Queued) === JobState.Running)
    assert(toJobState(QueuedHeld) === JobState.Running)
    assert(toJobState(Requeued) === JobState.Running)
    assert(toJobState(RequeuedHeld) === JobState.Running)
    assert(toJobState(Running) === JobState.Running)
    
    assert(toJobState(Suspended()) === JobState.Failed())
    assert(toJobState(Undetermined()) === JobState.Failed())
    assert(toJobState(Suspended(Some(resources))) === JobState.Failed(Some(resources)))
    assert(toJobState(Undetermined(Some(resources))) === JobState.Failed(Some(resources)))
  }
  
  
  test("isDone") {
    doFlagTest(
      _.isDone, 
      expectedTrueFor = Done, 
      expectedFalseFor = DoneUndetermined(), Failed(Some(resources)), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)), Undetermined())
  }
  
  test("isFailed") {
    doFlagTest(
      _.isFailed, 
      expectedTrueFor = Failed(), 
      expectedFalseFor = Done, DoneUndetermined(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)), Undetermined(Some(resources)))
  }
  
  test("isQueued") {
    doFlagTest(
      _.isQueued, 
      expectedTrueFor = Queued, 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }
  
  test("isQueuedHeld") {
    doFlagTest(
      _.isQueuedHeld, 
      expectedTrueFor = QueuedHeld, 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), Queued, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }
  
  test("isRunning") {
    doFlagTest(
      _.isRunning, 
      expectedTrueFor = Running, 
      expectedFalseFor = Done, DoneUndetermined(), Failed(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Suspended(Some(resources)), Undetermined(Some(resources)))
  }
  
  test("isSuspended") {
    doFlagTest(
      _.isSuspended, 
      expectedTrueFor = Suspended(), 
      expectedFalseFor = Done, DoneUndetermined(Some(resources)), Failed(Some(resources)), Queued, QueuedHeld, 
                         Requeued, RequeuedHeld, Running, Undetermined())
  }
  
  test("isUndetermined") {
    doFlagTest(
      _.isUndetermined, 
      expectedTrueFor = Undetermined(Some(resources)), 
      expectedFalseFor = Done, DoneUndetermined(), Failed(), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(Some(resources)))
  }
  
  test("isDoneUndetermined") {
    doFlagTest(
      _.isDoneUndetermined, 
      expectedTrueFor = DoneUndetermined(Some(resources)), 
      expectedFalseFor = Done, Failed(Some(resources)), Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended(), Undetermined())
  }

  test("notFinished") {
    assert(Queued.notFinished === true)
    assert(QueuedHeld.notFinished === true)
    assert(Running.notFinished === true)
    assert(Suspended().notFinished === true)
    assert(Undetermined().notFinished === true)
    
    assert(Done.notFinished === false)
    assert(DoneUndetermined().notFinished === false)
    assert(Failed().notFinished === false)
    //TODO: ???
    assert(Requeued.notFinished === false)
    //TODO: ???
    assert(RequeuedHeld.notFinished === false)
  }
  
  test("isFinished") {
    assert(Queued.isFinished === false)
    assert(QueuedHeld.isFinished === false)
    assert(Running.isFinished === false)
    assert(Suspended().isFinished === false)
    assert(Undetermined().isFinished === false)
    
    assert(Done.isFinished === true)
    assert(DoneUndetermined().isFinished === true)
    assert(Failed().isFinished === true)
    //TODO: ???
    assert(Requeued.isFinished === true)
    //TODO: ???
    assert(RequeuedHeld.isFinished === true)
  }
  
  private def doFlagTest(
      flag: UgerStatus => Boolean, 
      expectedTrueFor: UgerStatus, 
      expectedFalseFor: UgerStatus*): Unit = {
    
    assert(flag(expectedTrueFor) === true)
    
    for(ugerStatus <- expectedFalseFor) {
      assert(flag(ugerStatus) === false)
    }
  }
  
  //scalastyle:on magic.number
}
