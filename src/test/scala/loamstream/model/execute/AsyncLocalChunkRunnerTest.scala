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
  import loamstream.TestHelpers.waitForT
  
  test("executeSingle()") {
    import AsyncLocalChunkRunner.executeSingle
    import TestHelpers.runDataFromStatus
    import scala.concurrent.ExecutionContext.Implicits.global
    
    import JobStatus._
    
    val job = MockJob(Succeeded) 
    
    val failedJob = MockJob(Failed)
    
    val jobOracle = TestHelpers.DummyJobOracle
    
    val success = waitForT(executeSingle(ExecutionConfig.default, jobOracle, job))
    
    assert(success === runDataFromStatus(job, LocalSettings, Succeeded))
    
    val failure = waitForT(executeSingle(ExecutionConfig.default, jobOracle, failedJob))
    
    assert(failure === runDataFromStatus(failedJob, LocalSettings, Failed))
  }
  
  test("executeSingle() - job transitioned to right state") {
    import AsyncLocalChunkRunner.executeSingle
    import JobStatus._
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def doTest(expectedStatus: JobStatus): Unit = {
      val job = MockJob(expectedStatus)

      val jobOracle = TestHelpers.DummyJobOracle
      
      assert(job.executionCount === 0)
      
      val actualStatus0 = waitForT(executeSingle(ExecutionConfig.default, jobOracle, job)).jobStatus
      
      assert(job.executionCount === 1)
          
      assert(actualStatus0 === expectedStatus)
      
      val actualStatus1 = waitForT(executeSingle(ExecutionConfig.default, jobOracle, job)).jobStatus
      
      assert(job.executionCount === 2)
      
      assert(actualStatus1 === expectedStatus)
    }
    
    doTest(Failed)
    doTest(FailedWithException)
    doTest(Terminated)
    doTest(NotStarted)
    doTest(Skipped)
    doTest(Submitted)
    doTest(Succeeded)
  }
}
