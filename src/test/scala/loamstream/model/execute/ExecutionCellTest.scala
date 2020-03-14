package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult

/**
 * @author clint
 * Jan 30, 2020
 */
final class ExecutionCellTest extends FunSuite {
  import JobStatus._
  
  test("initial") {
    val expected = ExecutionCell(NotStarted, 0)
    
    assert(ExecutionCell.initial === expected) 
  }
  
  test("markAsRunning") {
    val cell0 = ExecutionCell.initial
    
    assert(cell0.status === NotStarted)
    assert(cell0.runCount === 0) 
    
    val cell1 = cell0.markAsRunning
    
    assert(cell1.status === Running)
    assert(cell1.runCount === 1)
    
    intercept[Exception] {
      cell1.markAsRunning
    }
    
    val cell2 = cell1.markAsRunnable.markAsRunning
    
    assert(cell2.status === Running)
    assert(cell2.runCount === 2)
  }
  
  test("markAsRunnable") {
    val cell0 = ExecutionCell.initial.markAsRunning
    
    assert(cell0.status === Running)
    
    val cell1 = cell0.markAsRunnable
    
    assert(cell1.status === NotStarted)
    
    val cell2 = cell1.markAsRunnable
    
    assert(cell2.status === NotStarted)
  }
  
  test("finishWith") {
    def doTest(
        status: JobStatus, 
        jobResult: Option[JobResult], 
        expectedStatus: JobStatus, 
        expectedResult: JobResult): Unit = {
      
      val cell0 = ExecutionCell.initial.markAsRunning
      
      assert(cell0.runCount === 1)
      
      val cell1 = cell0.finishWith(status)
      
      assert(cell1.status === expectedStatus)
      assert(cell1.runCount === 1)
    }
    
    doTest(Succeeded, None, Succeeded, JobResult.Success)
    doTest(Skipped, None, Skipped, JobResult.Success)
    doTest(Failed, None, Failed, JobResult.Failure)
    
    doTest(Succeeded, Some(JobResult.CommandResult(0)), Succeeded, JobResult.CommandResult(0))
    doTest(Failed, Some(JobResult.CommandResult(42)), Failed, JobResult.CommandResult(42))
  }
  
  test("markAs") {
    val cell0 = ExecutionCell.initial
    
    val statusesThatShouldBeDisallowed = JobStatus.values.filter(_.isFinished) - CouldNotStart
    
    statusesThatShouldBeDisallowed.foreach { s =>
      intercept[Exception] {
        cell0.markAs(s)
      }
    }
    
    val statusesThatShouldBeAllowed = JobStatus.values.filter(_.notFinished) + CouldNotStart
    
    statusesThatShouldBeAllowed.foreach { s => 
      val cell1 = cell0.markAs(s)
      
      assert(cell1.status === s)
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
  
  private def doPredicateTest(p: JobStatus => Boolean, cellField: ExecutionCell => Boolean): Unit = {
    val (matchingStatuses, nonMatchingStatuses) = JobStatus.values.partition(p)
    
    matchingStatuses.foreach { s =>
      val cell = ExecutionCell.initial.copy(status = s)
      
      assert(cellField(cell) === true)
    }
    
    nonMatchingStatuses.foreach { s =>
      val cell = ExecutionCell.initial.copy(status = s)
      
      assert(cellField(cell) === false)
    }
  }
}
