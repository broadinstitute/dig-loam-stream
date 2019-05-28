package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.TestJobs
import loamstream.util.Futures
import loamstream.util.Observables
import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.RunData


/**
 * @author clint
 * Nov 7, 2017
 */
final class AsyncLocalChunkRunnerTest extends FunSuite with TestJobs {
  import loamstream.TestHelpers.alwaysRestart
  import loamstream.TestHelpers.neverRestart
  import loamstream.TestHelpers.waitFor
  
  test("handleResultOfExecution") {
    import JobStatus._
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      import AsyncLocalChunkRunner.handleResultOfExecution
      
      {
        val job = MockJob(NotStarted)
      
        val runData = RunData(job, LocalSettings, status, None, None, None, None)
        
        assert(job.status === NotStarted)
        
        handleResultOfExecution(alwaysRestart)(runData) 
        
        assert(job.status === status)
      }
      
      {
        val job = MockJob(NotStarted)
        
        val runData = RunData(job, LocalSettings, status, None, None, None, None)
        
        assert(job.status === NotStarted)
      
        handleResultOfExecution(neverRestart)(runData)
      
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
  
  test("executeSingle()") {
    import AsyncLocalChunkRunner.executeSingle
    import TestHelpers.runDataFromStatus
    import scala.concurrent.ExecutionContext.Implicits.global
    
    import JobStatus._
    
    val job = MockJob(Succeeded) 
    
    val failedJob = MockJob(Failed)
    
    val jobOracle = TestHelpers.DummyJobOracle
    
    val success = waitFor(executeSingle(ExecutionConfig.default, jobOracle, job, neverRestart))
    
    assert(success === runDataFromStatus(job, LocalSettings, Succeeded))
    
    val failure = waitFor(executeSingle(ExecutionConfig.default, jobOracle, failedJob, neverRestart))
    
    assert(failure === runDataFromStatus(failedJob, LocalSettings, Failed))
    
    import Observables.Implicits._
    
    val two0StatusesFuture = job.statuses.take(3).to[Seq].firstAsFuture
    
    assert(waitFor(two0StatusesFuture) === Seq(Running, Succeeded))
  }
  
  test("executeSingle() - job transitioned to right state") {
    import AsyncLocalChunkRunner.executeSingle
    import JobStatus._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def doTest(status: JobStatus, expectedNoRestart: JobStatus): Unit = {
      val job = MockJob(status)

      val jobOracle = TestHelpers.DummyJobOracle
      
      //job.transitionTo(NotStarted)
      
      assert(job.executionCount === 0)
      
      assert(job.status === NotStarted)
      
      waitFor(executeSingle(ExecutionConfig.default, jobOracle, job, alwaysRestart))
      
      assert(job.executionCount === 1)
          
      assert(job.status === status)
      
      waitFor(executeSingle(ExecutionConfig.default, jobOracle, job, neverRestart))
      
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
