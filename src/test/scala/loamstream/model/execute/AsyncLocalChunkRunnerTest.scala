package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.TestJobs
import loamstream.util.Futures
import loamstream.util.ObservableEnrichments
import loamstream.conf.ExecutionConfig


/**
 * @author clint
 * Nov 7, 2017
 */
final class AsyncLocalChunkRunnerTest extends FunSuite with TestJobs {
  import loamstream.TestHelpers.alwaysRestart
  import loamstream.TestHelpers.neverRestart
  
  test("handleResultOfExecution") {
    import JobStatus._
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      import AsyncLocalChunkRunner.handleResultOfExecution
      
      {
        val job = MockJob(NotStarted)
      
        val execution = Execution.from(job, status)
        
        assert(job.status === NotStarted)
        
        handleResultOfExecution(alwaysRestart)(job -> execution) 
        
        assert(job.status === status)
      }
      
      {
        val job = MockJob(NotStarted)
        
        val execution = Execution.from(job, status)
        
        assert(job.status === NotStarted)
      
        handleResultOfExecution(neverRestart)(job -> execution)
      
        assert(job.status === expectedNoRestart)
      }
    }
    
    doTest(Failed, FailedPermanently)
    doTest(FailedWithException, FailedPermanently)
    doTest(Terminated, FailedPermanently)
    doTest(Skipped, Skipped)
    doTest(Submitted, Submitted)
    doTest(Succeeded, Succeeded)
  }
  
  test("determineFinalStatus") {
    import AsyncLocalChunkRunner.determineFinalStatus
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
  
  test("executeSingle()") {
    import AsyncLocalChunkRunner.executeSingle
    import TestHelpers.executionFromStatus
    import scala.concurrent.ExecutionContext.Implicits.global
    
    import JobStatus._
    
    val job = MockJob(Succeeded) 
    
    val failedJob = MockJob(Failed)
    
    val success = Futures.waitFor(executeSingle(ExecutionConfig.default, job, neverRestart))
    
    assert(success === (job -> executionFromStatus(Succeeded)))
    
    val failure = Futures.waitFor(executeSingle(ExecutionConfig.default, failedJob, neverRestart))
    
    assert(failure === (failedJob -> executionFromStatus(Failed)))
    
    import ObservableEnrichments._
    
    val two0StatusesFuture = job.statuses.take(3).to[Seq].firstAsFuture
    
    assert(Futures.waitFor(two0StatusesFuture) === Seq(Running, Succeeded))
  }
  
  test("executeSingle() - job transitioned to right state") {
    import AsyncLocalChunkRunner.executeSingle
    import JobStatus._
    import Futures.waitFor
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      val job = MockJob(status)
    
      //job.transitionTo(NotStarted)
      
      assert(job.executionCount === 0)
      
      assert(job.status === NotStarted)
      
      waitFor(executeSingle(ExecutionConfig.default, job, alwaysRestart))
      
      assert(job.executionCount === 1)
          
      assert(job.status === status)
      
      waitFor(executeSingle(ExecutionConfig.default, job, neverRestart))
      
      assert(job.executionCount === 2)
          
      assert(job.status === expectedNoRestart)
    }
    
    doTest(Failed, FailedPermanently)
    doTest(FailedWithException, FailedPermanently)
    doTest(Terminated, FailedPermanently)
    doTest(NotStarted, NotStarted)
    doTest(Skipped, Skipped)
    doTest(Submitted, Submitted)
    doTest(Succeeded, Succeeded)
  }
}
