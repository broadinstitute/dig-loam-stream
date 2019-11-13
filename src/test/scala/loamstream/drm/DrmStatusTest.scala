package loamstream.drm

import java.time.Instant

import org.scalatest.FunSuite

import loamstream.model.execute.Resources.UgerResources
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import java.time.LocalDateTime


/**
 * @author clint
 * Jan 5, 2017
 */
final class DrmStatusTest extends FunSuite {
  import DrmStatus._
  import scala.concurrent.duration._
    
  private val broadQueue = Queue("broad")
  
  private val resources = UgerResources(
      Memory.inGb(2), 
      CpuTime(1.second), 
      Some("example.com"), 
      Some(broadQueue),
      LocalDateTime.now,
      LocalDateTime.now)
      
  test("fromUgerStatusCode") {
    import org.ggf.drmaa.Session._
    
    assert(fromDrmStatusCode(QUEUED_ACTIVE) === Queued)
    assert(fromDrmStatusCode(SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(USER_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(USER_SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(RUNNING) === Running)
    assert(fromDrmStatusCode(SYSTEM_SUSPENDED) === Suspended)
    assert(fromDrmStatusCode(USER_SUSPENDED) === Suspended)
    assert(fromDrmStatusCode(USER_SYSTEM_SUSPENDED) === Suspended)
    assert(fromDrmStatusCode(DONE) === Done)
    assert(fromDrmStatusCode(FAILED) === Failed)
    assert(fromDrmStatusCode(UNDETERMINED) === Undetermined)
    assert(fromDrmStatusCode(-123456) === Undetermined)
    assert(fromDrmStatusCode(123456) === Undetermined)
    assert(fromDrmStatusCode(Int.MinValue) === Undetermined)
    assert(fromDrmStatusCode(Int.MaxValue) === Undetermined)
  }

  test("toJobStatus") {
    assert(toJobStatus(Done) === JobStatus.WaitingForOutputs)
    
    assert(toJobStatus(CommandResult(-1)) === JobStatus.Failed)
    assert(toJobStatus(CommandResult(0)) === JobStatus.WaitingForOutputs)
    assert(toJobStatus(CommandResult(42)) === JobStatus.Failed)
    
    assert(toJobStatus(DoneUndetermined) === JobStatus.Failed)
    assert(toJobStatus(Failed) === JobStatus.Failed)
    
    assert(toJobStatus(Queued) === JobStatus.Submitted)
    assert(toJobStatus(QueuedHeld) === JobStatus.Submitted)
    assert(toJobStatus(Requeued) === JobStatus.Submitted)
    assert(toJobStatus(RequeuedHeld) === JobStatus.Submitted)
    assert(toJobStatus(Running) === JobStatus.Running)
    
    assert(toJobStatus(Suspended) === JobStatus.Failed)
    assert(toJobStatus(Undetermined) === JobStatus.Unknown)
  }

  test("toJobResult") {
    assert(toJobResult(Done) === None)

    assert(toJobResult(CommandResult(-1)) === Some(JobResult.CommandResult(-1)))
    assert(toJobResult(CommandResult(0)) === Some(JobResult.CommandResult(0)))
    assert(toJobResult(CommandResult(42)) === Some(JobResult.CommandResult(42)))

    assert(toJobResult(DoneUndetermined) === Some(JobResult.Failure))
    assert(toJobResult(Failed) === Some(JobResult.Failure))
    assert(toJobResult(Suspended) === Some(JobResult.Failure))

    assert(toJobResult(Undetermined) === None)
    assert(toJobResult(Undetermined) === None)
    assert(toJobResult(Queued) === None)
    assert(toJobResult(QueuedHeld) === None)
    assert(toJobResult(Requeued) === None)
    assert(toJobResult(RequeuedHeld) === None)
    assert(toJobResult(Running) === None)
  }
  
  test("isDone") {
    doFlagTest(
      _.isDone, 
      expectedTrueFor = Done, 
      expectedFalseFor = DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isFailed") {
    doFlagTest(
      _.isFailed, 
      expectedTrueFor = Failed, 
      expectedFalseFor = Done, DoneUndetermined, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isQueued") {
    doFlagTest(
      _.isQueued, 
      expectedTrueFor = Queued, 
      expectedFalseFor = Done, DoneUndetermined, Failed, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isQueuedHeld") {
    doFlagTest(
      _.isQueuedHeld, 
      expectedTrueFor = QueuedHeld, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isRunning") {
    doFlagTest(
      _.isRunning, 
      expectedTrueFor = Running, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isSuspended") {
    doFlagTest(
      _.isSuspended, 
      expectedTrueFor = Suspended, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, 
                         Requeued, RequeuedHeld, Running, Undetermined, CommandResult(0))
  }
  
  test("isUndetermined") {
    doFlagTest(
      _.isUndetermined, 
      expectedTrueFor = Undetermined, 
      expectedFalseFor = Done, DoneUndetermined, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, CommandResult(0))
  }
  
  test("isDoneUndetermined") {
    doFlagTest(
      _.isDoneUndetermined, 
      expectedTrueFor = DoneUndetermined, 
      expectedFalseFor = Done, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, CommandResult(0))
  }
  
  test("isCommandResult") {
    doFlagTest(
      _.isCommandResult, 
      expectedTrueFor = CommandResult(0), 
      expectedFalseFor = Done, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, DoneUndetermined)
                         
    doFlagTest(
      _.isCommandResult, 
      expectedTrueFor = CommandResult(42), 
      expectedFalseFor = Done, Failed, Queued, QueuedHeld, Requeued, 
                         RequeuedHeld, Running, Suspended, Undetermined, DoneUndetermined)
  }

  test("notFinished") {
    assert(Requeued.notFinished === true)
    assert(RequeuedHeld.notFinished === true)
    assert(Queued.notFinished === true)
    assert(QueuedHeld.notFinished === true)
    assert(Running.notFinished === true)
    assert(Suspended.notFinished === true)
    assert(Undetermined.notFinished === true)
    
    assert(CommandResult(42).notFinished === false)
    assert(CommandResult(0).notFinished === false)

    assert(Done.notFinished === false)
    assert(DoneUndetermined.notFinished === false)
    assert(Failed.notFinished === false)
  }
  
  test("isFinished") {
    assert(Queued.isFinished === false)
    assert(QueuedHeld.isFinished === false)
    assert(Requeued.isFinished === false)
    assert(RequeuedHeld.isFinished === false)
    assert(Running.isFinished === false)
    assert(Suspended.isFinished === false)
    assert(Undetermined.isFinished === false)

    assert(CommandResult(0).isFinished === true)
    assert(CommandResult(42).isFinished === true)

    assert(Done.isFinished === true)
    assert(DoneUndetermined.isFinished === true)
    assert(Failed.isFinished === true)
  }
  
  private def doFlagTest(
      flag: DrmStatus => Boolean, 
      expectedTrueFor: DrmStatus, 
      expectedFalseFor: DrmStatus*): Unit = {
    
    assert(flag(expectedTrueFor) === true)
    
    for(ugerStatus <- expectedFalseFor) {
      assert(flag(ugerStatus) === false)
    }
  }
}
