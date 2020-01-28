package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult

/**
 * @author clint
 * Jan 24, 2020
 */
final case class ExecutionCell(
    status: JobStatus = JobStatus.NotStarted, 
    result: Option[JobResult] = None, 
    runCount: Int = 0) {
  
  def startRunning: ExecutionCell = copy(status = JobStatus.Running, runCount = runCount + 1)
  
  def markAsRunnable: ExecutionCell = copy(status = JobStatus.NotStarted)
  
  def finishWith(status: JobStatus, jobResult: Option[JobResult] = None): ExecutionCell = {
    require(status.isFinished, s"Expected finished job status, but got $status")
    
    val newResult = jobResult.orElse(Some(if(status.isSuccess) JobResult.Success else JobResult.Failure))
    
    copy(status = status, result = newResult)
  }
  
  def markAs(status: JobStatus): ExecutionCell = {
    require(status.notFinished || status.isCouldNotStart, s"Expected a non-finished job status, but got $status")
    
    copy(status = status)
  }
  
  def isFailure: Boolean = status.isFailure
  
  def canStopExecution: Boolean = status.canStopExecution
  
  def isFinished: Boolean = status.isFinished
  
  def isTerminal: Boolean = status.isTerminal
  def nonTerminal: Boolean = !isTerminal
  
  def notStarted: Boolean = status == JobStatus.NotStarted
}

object ExecutionCell {
  val initial: ExecutionCell = ExecutionCell()
}
