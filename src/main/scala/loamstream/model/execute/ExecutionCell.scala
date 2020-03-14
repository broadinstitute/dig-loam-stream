package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult

/**
 * @author clint
 * Jan 24, 2020
 * 
 * A snapshot of an LJob's state at a particular moment during execution, from the perspective of RxExecuter and
 * ExecutionState.  Tracks the jobs status and the number of times it has ran. 
 */
final case class ExecutionCell(status: JobStatus = JobStatus.NotStarted, runCount: Int = 0) {
  
  def markAsRunning: ExecutionCell = {
    require(status == JobStatus.NotStarted, s"Expected status to be ${JobStatus.NotStarted} but got $status")
    
    copy(status = JobStatus.Running, runCount = runCount + 1)
  }
  
  def markAsRunnable: ExecutionCell = copy(status = JobStatus.NotStarted)
  
  def finishWith(newStatus: JobStatus): ExecutionCell = {
    require(newStatus.isFinished, s"Expected finished job status, but got $status")
    
    copy(status = newStatus)
  }
  
  def markAs(newStatus: JobStatus): ExecutionCell = {
    require(newStatus.notFinished || newStatus.isCouldNotStart, s"Expected a non-finished job status, but got $status")
    
    copy(status = newStatus)
  }
  
  def isFailure: Boolean = status.isFailure
  
  def canStopExecution: Boolean = status.canStopExecution
  
  def isFinished: Boolean = status.isFinished
  
  def isTerminal: Boolean = status.isTerminal
  def nonTerminal: Boolean = !isTerminal
  
  def notStarted: Boolean = status == JobStatus.NotStarted
  
  def isRunning: Boolean = status.isRunning
  
  def isSkipped: Boolean = status.isSkipped
  
  def couldNotStart: Boolean = status.isCouldNotStart
}

object ExecutionCell {
  val initial: ExecutionCell = ExecutionCell()
}
