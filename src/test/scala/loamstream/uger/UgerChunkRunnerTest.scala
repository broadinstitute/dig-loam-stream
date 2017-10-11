package loamstream.uger

import java.nio.file.Paths
import loamstream.conf.UgerConfig
import loamstream.model.jobs.NoOpJob
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import org.scalatest.FunSuite
import rx.lang.scala.schedulers.IOScheduler
import loamstream.TestHelpers
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.LJob
import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.jobs.Output
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import loamstream.model.jobs.Execution
import rx.lang.scala.Observable
import loamstream.model.quantities.Memory

/**
  * Created by kyuksel on 7/25/16.
  */
final class UgerChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  private val scheduler = IOScheduler()
  
  import TestHelpers.neverRestart
  
  private val config = {
    import scala.concurrent.duration.DurationInt
    
    UgerConfig(
      workDir = Paths.get("target/foo"), 
      logFile = Paths.get("target/bar"), 
      nativeSpecification = "some job parameters", 
      maxNumJobs = 42,
      defaultCores = 2,
      defaultMemoryPerCore = Memory.inGb(2),
      defaultMaxRunTime = 2.hours)
  }
  private val client = MockDrmaaClient(Map.empty)
  private val runner = UgerChunkRunner(config, client, new JobMonitor(scheduler, Poller.drmaa(client)))

  import Futures.waitFor
  import ObservableEnrichments._
  
  test("NoOpJob is not attempted to be executed") {
    val noOpJob = NoOpJob(Set.empty)
    val result = waitFor(runner.run(Set(noOpJob), neverRestart).firstAsFuture)
    assert(result === Map.empty)
  }

  test("No failures when empty set of jobs is presented") {
    val result = waitFor(runner.run(Set.empty, neverRestart).firstAsFuture)
    assert(result === Map.empty)
  }
  
  test("combine") {
    import UgerChunkRunner.combine
    
    assert(combine(Map.empty, Map.empty) == Map.empty)
    
    val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    
    assert(combine(Map.empty, m1) == Map.empty)
    
    assert(combine(m1, Map.empty) == Map.empty)
    
    val m2 = Map("a" -> 42.0, "c" -> 99.0, "x" -> 123.456)
    
    assert(combine(m1, m2) == Map("a" -> (1, 42.0), "c" -> (3, 99.0)))
    
    assert(combine(m2, m1) == Map("a" -> (42.0, 1), "c" -> (99.0, 3)))
  }
  
  test("handleFailureStatus") {
    import UgerChunkRunner.handleFailureStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(failureStatus: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      handleFailureStatus(alwaysRestart, failureStatus)(job)
      
      assert(job.status === failureStatus)
      
      handleFailureStatus(neverRestart, failureStatus)(job)
      
      assert(job.status === FailedPermanently)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
  }

  test("handleUgerStatus") {
    import UgerChunkRunner.handleUgerStatus
    import JobStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    
    def doTest(ugerStatus: UgerStatus, isFailure: Boolean): Unit = {
      val job = MockJob(NotStarted)
      
      val jobStatus = UgerStatus.toJobStatus(ugerStatus)
      
      assert(job.status === NotStarted)
      
      handleUgerStatus(alwaysRestart, job)(ugerStatus)
      
      assert(job.status === jobStatus)
      
      handleUgerStatus(neverRestart, job)(ugerStatus)
      
      val expected = if(isFailure) FailedPermanently else jobStatus
      
      assert(job.status === expected)
    }
    
    doTest(UgerStatus.Failed(), isFailure = true)
    doTest(UgerStatus.CommandResult(1, None), isFailure = true)
    doTest(UgerStatus.DoneUndetermined(), isFailure = true)
    doTest(UgerStatus.Suspended(), isFailure = true)
    
    doTest(UgerStatus.Done, isFailure = false)
    doTest(UgerStatus.Queued, isFailure = false)
    doTest(UgerStatus.QueuedHeld, isFailure = false)
    doTest(UgerStatus.Requeued, isFailure = false)
    doTest(UgerStatus.RequeuedHeld, isFailure = false)
    doTest(UgerStatus.Running, isFailure = false)
    doTest(UgerStatus.Undetermined(), isFailure = false)
    doTest(UgerStatus.CommandResult(0, None), isFailure = false)
  }
  
  private def toTuple(job: UgerChunkRunnerTest.MockUgerJob): (LJob, Observable[UgerStatus]) = {
    job -> Observable.from(job.statusesToReturn)
  }
  
  test("toExecutions - one failed job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val id = "failed"
    
    def doTest(shouldRestart: LJob => Boolean, lastUgerStatus: UgerStatus, expectedLastStatus: JobStatus): Unit = {
      val failed = MockUgerJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      assert(failed.runCount === 0)
      
      val result = waitFor(toExecutions(shouldRestart, Map(id -> toTuple(failed))).firstAsFuture)
      
      val Seq((actualJob, execution)) = result.toSeq
      
      assert(actualJob === failed)    
      assert(execution.status === JobStatus.Failed)
      assert(execution.isFailure)
      //TODO: Other assertions about execution?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      val statuses = if(shouldRestart(failed)) failed.statuses.take(3) else failed.statuses 

      assert(waitFor(statuses.to[Seq].firstAsFuture) === expectedStatuses)
      
      assert(failed.runCount === 1)
    }
    
    doTest(neverRestart, Failed(), JobStatus.FailedPermanently)
    doTest(neverRestart, DoneUndetermined(), JobStatus.FailedPermanently)
    doTest(neverRestart, CommandResult(1, None), JobStatus.FailedPermanently)
    
    doTest(alwaysRestart, Failed(), JobStatus.Failed)
    doTest(alwaysRestart, DoneUndetermined(), JobStatus.Failed)
    doTest(alwaysRestart, CommandResult(1, None), JobStatus.Failed)
  }
  
  test("toExecutions - one successful job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val id = "worked"
    
    def doTest(shouldRestart: LJob => Boolean, lastUgerStatus: UgerStatus, expectedLastStatus: JobStatus): Unit = {
      val worked = MockUgerJob(id, Queued, Queued, Running, Running, lastUgerStatus)
      
      assert(worked.runCount === 0)
      
      val result = waitFor(toExecutions(shouldRestart, Map(id -> toTuple(worked))).firstAsFuture)
      
      val Seq((actualJob, execution)) = result.toSeq
      
      assert(actualJob === worked)
      assert(execution.status === JobStatus.Succeeded)
      assert(execution.isSuccess)
      //TODO: Other assertions about execution?
      
      val expectedStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastStatus)
      
      val statuses = worked.statuses.to[Seq] 

      assert(waitFor(statuses.firstAsFuture) === expectedStatuses)
      
      assert(worked.runCount === 1)
    }
    
    doTest(neverRestart, Done, JobStatus.Succeeded)
    doTest(neverRestart, CommandResult(0, None), JobStatus.Succeeded)
    
    doTest(alwaysRestart, Done, JobStatus.Succeeded)
    doTest(alwaysRestart, CommandResult(0, None), JobStatus.Succeeded)
  }
  
  test("toExecutions - one successful job, one failed job") {
    import UgerChunkRunner.toExecutions
    import UgerStatus._
    import TestHelpers.{alwaysRestart, neverRestart}
    import UgerChunkRunnerTest.MockUgerJob
    import ObservableEnrichments._
    
    val goodId = "worked"
    val badId = "failed"
    
    def doTest(
        shouldRestart: LJob => Boolean, 
        lastGoodUgerStatus: UgerStatus, 
        lastBadUgerStatus: UgerStatus, 
        expectedLastGoodStatus: JobStatus,
        expectedLastBadStatus: JobStatus): Unit = {
      
      val worked = MockUgerJob(goodId, Queued, Queued, Running, Running, lastGoodUgerStatus)
      val failed = MockUgerJob(badId, Queued, Queued, Running, Running, lastBadUgerStatus)
      
      assert(worked.runCount === 0)
      assert(failed.runCount === 0)
      
      val input = Map(goodId -> toTuple(worked), badId -> toTuple(failed))
      
      val result = waitFor(toExecutions(shouldRestart, input).firstAsFuture)
      
      val goodExecution = result(worked)
      val badExecution = result(failed)
      
      assert(result.size === 2)
      
      assert(goodExecution.status === JobStatus.Succeeded)
      assert(goodExecution.isSuccess)
      assert(badExecution.status === JobStatus.Failed)
      assert(badExecution.isFailure)
      //TODO: Other assertions about execution?
      
      val expectedGoodStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastGoodStatus)
      val expectedBadStatuses = Seq(JobStatus.Submitted, JobStatus.Running, expectedLastBadStatus)
      
      val goodStatuses = worked.statuses.to[Seq] 
      val badStatuses = (if(shouldRestart(failed)) failed.statuses.take(3) else failed.statuses).to[Seq]

      assert(waitFor(goodStatuses.firstAsFuture) === expectedGoodStatuses)
      assert(waitFor(badStatuses.firstAsFuture) === expectedBadStatuses)
      
      assert(worked.runCount === 1)
      assert(failed.runCount === 1)
    }
    
    val failedPermanently = JobStatus.FailedPermanently
    
    doTest(neverRestart, Done, Failed(), JobStatus.Succeeded, failedPermanently)
    doTest(neverRestart, CommandResult(0, None), CommandResult(1, None), JobStatus.Succeeded, failedPermanently)
    
    doTest(alwaysRestart, Done, Failed(), JobStatus.Succeeded, JobStatus.Failed)
    doTest(alwaysRestart, CommandResult(0, None), CommandResult(1, None), JobStatus.Succeeded, JobStatus.Failed)
  }
  
  //scalastyle:on magic.number
}

object UgerChunkRunnerTest {
  final case class MockUgerJob(name: String, statusesToReturn: UgerStatus*) extends LJob {
    require(statusesToReturn.nonEmpty)

    override val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.Local
    
    override val inputs: Set[LJob] = Set.empty

    override val outputs: Set[Output] = Set.empty
    
    override def execute(implicit context: ExecutionContext): Future[Execution] = {
      Future.successful(Execution.from(this, UgerStatus.toJobStatus(statusesToReturn.last)))
    }

    protected def doWithInputs(newInputs: Set[LJob]): LJob = ???
  }
}
