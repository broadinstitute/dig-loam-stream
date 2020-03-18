package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.MockJob
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jan 30, 2020
 */
final class JobExecutionStateTest extends FunSuite {
  import JobStatus._
  
  private def mockJob: LJob = MockJob(JobStatus.Succeeded)
  
  test("initial") {
    val job = mockJob
    
    val expected = JobExecutionState(job, NotStarted, 0)
    
    assert(JobExecutionState.initialFor(job) === expected) 
  }
  
  test("markAsRunning") {
    val job = mockJob
    
    val state0 = JobExecutionState.initialFor(job)
    
    assert(state0.status === NotStarted)
    assert(state0.runCount === 0) 
    
    val state1 = state0.markAsRunning
    
    assert(state1.status === Running)
    assert(state1.runCount === 1)
    
    intercept[Exception] {
      state1.markAsRunning
    }
    
    val state2 = state1.markAsRunnable.markAsRunning
    
    assert(state2.status === Running)
    assert(state2.runCount === 2)
  }
  
  test("markAsRunnable") {
    val job = mockJob
    
    val state0 = JobExecutionState.initialFor(job).markAsRunning
    
    assert(state0.status === Running)
    
    val state1 = state0.markAsRunnable
    
    assert(state1.status === NotStarted)
    
    val state2 = state1.markAsRunnable
    
    assert(state2.status === NotStarted)
  }
  
  test("finishWith") {
    def doTest(
        status: JobStatus, 
        jobResult: Option[JobResult], 
        expectedStatus: JobStatus, 
        expectedResult: JobResult): Unit = {
      
      val job = mockJob 
      
      val state0 = JobExecutionState.initialFor(job).markAsRunning
      
      assert(state0.runCount === 1)
      
      val state1 = state0.finishWith(status)
      
      assert(state1.status === expectedStatus)
      assert(state1.runCount === 1)
    }
    
    doTest(Succeeded, None, Succeeded, JobResult.Success)
    doTest(Skipped, None, Skipped, JobResult.Success)
    doTest(Failed, None, Failed, JobResult.Failure)
    
    doTest(Succeeded, Some(JobResult.CommandResult(0)), Succeeded, JobResult.CommandResult(0))
    doTest(Failed, Some(JobResult.CommandResult(42)), Failed, JobResult.CommandResult(42))
  }
  
  test("markAs") {
    val job = mockJob
    
    val state0 = JobExecutionState.initialFor(job)
    
    val statusesThatShouldBeDisallowed = JobStatus.values.filter(_.isFinished) - CouldNotStart
    
    statusesThatShouldBeDisallowed.foreach { s =>
      intercept[Exception] {
        state0.markAs(s)
      }
    }
    
    val statusesThatShouldBeAllowed = JobStatus.values.filter(_.notFinished) + CouldNotStart
    
    statusesThatShouldBeAllowed.foreach { s => 
      val state1 = state0.markAs(s)
      
      assert(state1.status === s)
    }
  }
  
  test("isFailure") {
    doPredicateTest(_.isFailure, _.isFailure)
  }
  
  test("canStopExecution") {
    doPredicateTest(_.canStopExecution, _.canStopExecution)
  }
  
  test("isFinished") {
    doPredicateTest(_.isFinished, _.isFinished)
  }
  
  test("isTerminal") {
    doPredicateTest(_.isTerminal, _.isTerminal)
  }
  
  test("nonTerminal") {
    doPredicateTest(!_.isTerminal, _.nonTerminal)
  }
  
  test("notStarted") {
    doPredicateTest(_ == NotStarted, _.notStarted)
  }
  
  private def doPredicateTest(p: JobStatus => Boolean, cellField: JobExecutionState => Boolean): Unit = {
    val (matchingStatuses, nonMatchingStatuses) = JobStatus.values.partition(p)
    
    matchingStatuses.foreach { s =>
      val state = JobExecutionState.initialFor(mockJob).copy(status = s)
      
      assert(cellField(state) === true)
    }
    
    nonMatchingStatuses.foreach { s =>
      val state = JobExecutionState.initialFor(mockJob).copy(status = s)
      
      assert(cellField(state) === false)
    }
  }
}
