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

/**
  * Created by kyuksel on 7/25/16.
  */
final class UgerChunkRunnerTest extends FunSuite {
  //scalastyle:off magic.number
  
  private val scheduler = IOScheduler()
  
  import TestHelpers.neverRestart
  
  private val config = UgerConfig(Paths.get("target/foo"), Paths.get("target/bar"), "some job parameters", 42)
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
  
  //scalastyle:on magic.number
}
