package loamstream.model.execute

import loamstream.TestHelpers
import org.scalatest.FunSuite

import scala.concurrent.Await
import loamstream.model.jobs.{JobStatus, RxMockJob, TestJobs}

import scala.concurrent.duration.Duration
import loamstream.util.ObservableEnrichments
import loamstream.util.Futures
import loamstream.model.jobs.LJob
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.Execution

/**
 * @author clint
 * date: Jun 7, 2016
 */
// scalastyle:off magic.number
final class ExecuterHelpersTest extends FunSuite with TestJobs {
  
  import TestHelpers.alwaysRestart
  import TestHelpers.neverRestart
  
  test("handleResultOfExecution") {
    import ExecuterHelpers.handleResultOfExecution
    import JobStatus._
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      val job = MockJob(NotStarted)
    
      val execution = Execution.from(job, status)
      
      assert(job.status === NotStarted)
      
      handleResultOfExecution(alwaysRestart)(job -> execution) 
      
      assert(job.status === status)
      
      handleResultOfExecution(neverRestart)(job -> execution)
      
      assert(job.status === expectedNoRestart)
    }
    
    doTest(Failed, FailedPermanently)
    doTest(FailedWithException, FailedPermanently)
    doTest(Terminated, FailedPermanently)
    doTest(NotStarted, NotStarted)
    doTest(Running, Running)
    doTest(Skipped, Skipped)
    doTest(Submitted, Submitted)
    doTest(Succeeded, Succeeded)
  }
  
  test("determineFinalStatus") {
    import ExecuterHelpers.determineFinalStatus
    import JobStatus._
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      assert(determineFinalStatus(alwaysRestart, status, job) === status)
      
      assert(determineFinalStatus(neverRestart, status, job) === expectedNoRestart)
      
      assert(job.status === NotStarted)
    }
    
    doTest(Failed, FailedPermanently)
    doTest(FailedWithException, FailedPermanently)
    doTest(Terminated, FailedPermanently)
    doTest(NotStarted, NotStarted)
    doTest(Running, Running)
    doTest(Skipped, Skipped)
    doTest(Submitted, Submitted)
    doTest(Succeeded, Succeeded)
  }
  
  test("determineFailureStatus") {
    import ExecuterHelpers.determineFailureStatus
    import JobStatus._
    
    def doTest(failureStatus: JobStatus): Unit = {
      val job = MockJob(NotStarted)
      
      assert(job.status === NotStarted)
      
      assert(determineFailureStatus(alwaysRestart, failureStatus, job) === failureStatus)
      
      assert(determineFailureStatus(neverRestart, failureStatus, job) === FailedPermanently)
      
      assert(job.status === NotStarted)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
  }
  
  test("flattenTree") {
    import ExecuterHelpers.flattenTree
    
    val noDeps0 = RxMockJob("noDeps")
    
    assert(flattenTree(Set(noDeps0)) == Set(noDeps0))
    
    val middle0 = RxMockJob("middle", Set(noDeps0))
    
    assert(flattenTree(Set(middle0)) == Set(middle0, noDeps0))
    
    val root0 = RxMockJob("root", Set(middle0))
    
    assert(flattenTree(Set(root0)) == Set(root0, middle0, noDeps0))
    
    val noDeps1 = RxMockJob("noDeps1")
    val middle1 = RxMockJob("middle1", Set(noDeps1))
    val root1 = RxMockJob("root1", Set(middle1))
    
    assert(flattenTree(Set(root0, root1)) == Set(root0, middle0, noDeps0, root1, middle1, noDeps1))
  }
  
  test("executeSingle()") {
    import ExecuterHelpers.executeSingle
    import TestHelpers.executionFromStatus
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val success = Await.result(executeSingle(two0, neverRestart), Duration.Inf)
    
    assert(success === (two0 -> executionFromStatus(two0Success)))
    
    val failure = Await.result(executeSingle(two0Failed, neverRestart), Duration.Inf)
    
    assert(failure === (two0Failed -> executionFromStatus(two0Failure)))
    
    import ObservableEnrichments._
    
    val two0StatusesFuture = two0.statuses.take(3).to[Seq].firstAsFuture
    
    import JobStatus._
    
    assert(Futures.waitFor(two0StatusesFuture) === Seq(NotStarted, Running, Succeeded))
  }
  
  test("executeSingle() - job transitioned to right state") {
    import ExecuterHelpers.executeSingle
    import JobStatus._
    import Futures.waitFor
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      val job = MockJob(status)
    
      job.transitionTo(NotStarted)
      
      assert(job.executionCount === 0)
      
      assert(job.status === NotStarted)
      
      waitFor(executeSingle(job, alwaysRestart))
      
      assert(job.executionCount === 1)
          
      assert(job.status === status)
      
      waitFor(executeSingle(job, neverRestart))
      
      assert(job.executionCount === 2)
          
      assert(job.status === expectedNoRestart)
    }
    
    doTest(Failed, FailedPermanently)
    doTest(FailedWithException, FailedPermanently)
    doTest(Terminated, FailedPermanently)
    doTest(NotStarted, NotStarted)
    doTest(Running, Running)
    doTest(Skipped, Skipped)
    doTest(Submitted, Submitted)
    doTest(Succeeded, Succeeded)
  }
  
  test("noFailures() and anyFailures()") {
    import ExecuterHelpers.{noFailures,anyFailures}

    assert(noFailures(Map.empty) === true)
    assert(anyFailures(Map.empty) === false)

    val allSuccesses = Map( two0 -> two0Success,
                            two1 -> two1Success,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> plusOneSuccess).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allSuccesses) === true)
    assert(anyFailures(allSuccesses) === false)
    
    val allFailures = Map(
                          two0 -> JobStatus.Failed,
                          two1 -> JobStatus.Failed,
                          twoPlusTwo -> JobStatus.Failed,
                          plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(allFailures) === false)
    assert(anyFailures(allFailures) === true)
    
    val someFailures = Map(
                            two0 -> two0Success,
                            two1 -> JobStatus.Failed,
                            twoPlusTwo -> twoPlusTwoSuccess,
                            plusOne -> JobStatus.Failed).mapValues(TestHelpers.executionFrom(_))
      
    assert(noFailures(someFailures) === false)
    assert(anyFailures(someFailures) === true)
  }
}
// scalastyle:on magic.number
