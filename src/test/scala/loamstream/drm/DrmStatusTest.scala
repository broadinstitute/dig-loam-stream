package loamstream.drm

import java.time.Instant

import org.ggf.drmaa.Session

import org.scalatest.FunSuite

import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobStatus
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory


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
      Instant.now,
      Instant.now)
  
  test("transformResources") {
    def doTestWithExistingResources(makeInitialStatus: Option[DrmResources] => DrmStatus): Unit = {
      val initialStatus = makeInitialStatus(Some(resources))
      
      assert(initialStatus.resourcesOpt !== None)
      assert(initialStatus.resourcesOpt.get.queue === Some(broadQueue))
      
      val transformed = initialStatus.transformResources(_.withQueue(broadQueue))
      
      assert(transformed.getClass === initialStatus.getClass)
      
      assert(initialStatus.resourcesOpt !== None)
      assert(initialStatus.resourcesOpt.get.queue === Some(broadQueue))
      
      assert(transformed.resourcesOpt.get.queue === Some(broadQueue))
    }
    
    doTestWithExistingResources(CommandResult(0, _))
    doTestWithExistingResources(CommandResult(1, _))
    doTestWithExistingResources(CommandResult(42, _))
    doTestWithExistingResources(Failed( _))
    doTestWithExistingResources(DoneUndetermined(_))
    doTestWithExistingResources(Suspended(_))
    doTestWithExistingResources(Undetermined(_))
    
    def doTestWithoutResources(initialStatus: DrmStatus): Unit = {
      assert(initialStatus.resourcesOpt === None)
      
      val transformed = initialStatus.transformResources(_.withQueue(broadQueue))
      
      assert(initialStatus.resourcesOpt === None)
      
      assert(transformed.resourcesOpt === None)
      
      assert(transformed.getClass === initialStatus.getClass)
      assert(transformed eq initialStatus)
    }
   
    doTestWithoutResources(Done)
    doTestWithoutResources(Queued)
    doTestWithoutResources(QueuedHeld)
    doTestWithoutResources(Requeued)
    doTestWithoutResources(RequeuedHeld)
    doTestWithoutResources(Running)
  }
      
  test("fromUgerStatusCode") {
    import Session._
    
    assert(fromDrmStatusCode(QUEUED_ACTIVE) === Queued)
    assert(fromDrmStatusCode(SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(USER_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(USER_SYSTEM_ON_HOLD) === QueuedHeld)
    assert(fromDrmStatusCode(RUNNING) === Running)
    assert(fromDrmStatusCode(SYSTEM_SUSPENDED) === Suspended())
    assert(fromDrmStatusCode(USER_SUSPENDED) === Suspended())
    assert(fromDrmStatusCode(USER_SYSTEM_SUSPENDED) === Suspended())
    assert(fromDrmStatusCode(DONE) === Done)
    assert(fromDrmStatusCode(FAILED) === Failed())
    assert(fromDrmStatusCode(UNDETERMINED) === Undetermined())
    assert(fromDrmStatusCode(-123456) === Undetermined())
    assert(fromDrmStatusCode(123456) === Undetermined())
    assert(fromDrmStatusCode(Int.MinValue) === Undetermined())
    assert(fromDrmStatusCode(Int.MaxValue) === Undetermined())
  }

  test("toJobStatus") {
    assert(toJobStatus(Done) === JobStatus.Succeeded)
    
    assert(toJobStatus(CommandResult(-1, Some(resources))) === JobStatus.Failed)
    assert(toJobStatus(CommandResult(0, Some(resources))) === JobStatus.Succeeded)
    assert(toJobStatus(CommandResult(42, Some(resources))) === JobStatus.Failed)
    
    assert(toJobStatus(DoneUndetermined(Some(resources))) === JobStatus.Failed)
    assert(toJobStatus(Failed()) === JobStatus.Failed)
    assert(toJobStatus(Failed(Some(resources))) === JobStatus.Failed)
    
    assert(toJobStatus(Queued) === JobStatus.Submitted)
    assert(toJobStatus(QueuedHeld) === JobStatus.Submitted)
    assert(toJobStatus(Requeued) === JobStatus.Submitted)
    assert(toJobStatus(RequeuedHeld) === JobStatus.Submitted)
    assert(toJobStatus(Running) === JobStatus.Running)
    
    assert(toJobStatus(Suspended()) === JobStatus.Failed)
    assert(toJobStatus(Undetermined()) === JobStatus.Unknown)
    assert(toJobStatus(Suspended(Some(resources))) === JobStatus.Failed)
    assert(toJobStatus(Undetermined(Some(resources))) === JobStatus.Unknown)
  }

  test("toJobResult") {
    assert(toJobResult(Done) === None)

    assert(toJobResult(CommandResult(-1, Some(resources))) === Some(JobResult.CommandResult(-1)))
    assert(toJobResult(CommandResult(-1, None)) === Some(JobResult.CommandResult(-1)))
    assert(toJobResult(CommandResult(0, Some(resources))) === Some(JobResult.CommandResult(0)))
    assert(toJobResult(CommandResult(0, None)) === Some(JobResult.CommandResult(0)))
    assert(toJobResult(CommandResult(42, Some(resources))) === Some(JobResult.CommandResult(42)))
    assert(toJobResult(CommandResult(42, None)) === Some(JobResult.CommandResult(42)))

    assert(toJobResult(DoneUndetermined(Some(resources))) === Some(JobResult.Failure))
    assert(toJobResult(DoneUndetermined(None)) === Some(JobResult.Failure))
    assert(toJobResult(Failed(Some(resources))) === Some(JobResult.Failure))
    assert(toJobResult(Failed(None)) === Some(JobResult.Failure))
    assert(toJobResult(Failed()) === Some(JobResult.Failure))
    assert(toJobResult(Suspended(Some(resources))) === Some(JobResult.Failure))
    assert(toJobResult(Suspended(None)) === Some(JobResult.Failure))
    assert(toJobResult(Suspended()) === Some(JobResult.Failure))

    assert(toJobResult(Undetermined(Some(resources))) === None)
    assert(toJobResult(Undetermined()) === None)
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
    assert(Requeued.notFinished === true)
    assert(RequeuedHeld.notFinished === true)
    assert(Queued.notFinished === true)
    assert(QueuedHeld.notFinished === true)
    assert(Running.notFinished === true)
    assert(Suspended().notFinished === true)
    assert(Suspended(Some(resources)).notFinished === true)
    assert(Undetermined().notFinished === true)
    assert(Undetermined(Some(resources)).notFinished === true)
    
    assert(CommandResult(42, None).notFinished === false)
    assert(CommandResult(42, Some(resources)).notFinished === false)
    assert(CommandResult(0, None).notFinished === false)
    assert(CommandResult(0, Some(resources)).notFinished === false)

    assert(Done.notFinished === false)
    assert(DoneUndetermined().notFinished === false)
    assert(DoneUndetermined(Some(resources)).notFinished === false)
    assert(Failed().notFinished === false)
    assert(Failed(Some(resources)).notFinished === false)
  }
  
  test("isFinished") {
    assert(Queued.isFinished === false)
    assert(QueuedHeld.isFinished === false)
    assert(Requeued.isFinished === false)
    assert(RequeuedHeld.isFinished === false)
    assert(Running.isFinished === false)
    assert(Suspended().isFinished === false)
    assert(Suspended(Some(resources)).isFinished === false)
    assert(Undetermined().isFinished === false)
    assert(Undetermined(Some(resources)).isFinished === false)

    assert(CommandResult(0, None).isFinished === true)
    assert(CommandResult(0, Some(resources)).isFinished === true)
    assert(CommandResult(42, None).isFinished === true)
    assert(CommandResult(42, Some(resources)).isFinished === true)

    assert(Done.isFinished === true)
    assert(DoneUndetermined().isFinished === true)
    assert(DoneUndetermined(Some(resources)).isFinished === true)
    assert(Failed().isFinished === true)
    assert(Failed(Some(resources)).isFinished === true)
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
  
  //scalastyle:on magic.number
}
