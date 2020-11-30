package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult
import JobExecutionState.encode
import JobExecutionState.decode


/**
 * @author clint
 * Jan 24, 2020
 * 
 * A snapshot of an LJob's state at a particular moment during execution, from the perspective of RxExecuter and
 * ExecutionState.  Tracks the jobs status and the number of times it has ran. 
 */
final class JobExecutionState(
    val job: LJob, 
    private val encodedStatus: Byte, 
    val runCount: Byte) {
  
  def status: JobStatus = decode(encodedStatus)
  
  def doCopy(
      job: LJob = this.job, 
      encodedStatus: Byte = this.encodedStatus, 
      runCount: Byte = this.runCount): JobExecutionState = new JobExecutionState(job = job, encodedStatus = encodedStatus, runCount = runCount)
  
  def copy(
      job: LJob = this.job, 
      status: JobStatus = decode(this.encodedStatus), 
      runCount: Int = this.runCount.toInt)(implicit discriminator: Int = 42): JobExecutionState = new JobExecutionState(job = job, encodedStatus = encode(status), runCount = runCount.toByte)

  override def hashCode: Int = Seq(job, encodedStatus, runCount).hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: JobExecutionState => {
      this.job.id == that.job.id &&
      this.encodedStatus == that.encodedStatus &&
      this.runCount == that.runCount
    }
    case _ => false
  }
  
  def markAsRunning: JobExecutionState = {
    require(status == JobStatus.NotStarted, s"Expected status to be ${JobStatus.NotStarted} but got $status")
    
    copy(status = JobStatus.Running, runCount = (runCount + 1).toByte)
  }
  
  def markAsRunnable: JobExecutionState = copy(status = JobStatus.NotStarted)
  
  def finishWith(newStatus: JobStatus): JobExecutionState = {
    require(newStatus.isFinished, s"Expected finished job status, but got $status")
    
    copy(status = newStatus)
  }
  
  def markAs(newStatus: JobStatus): JobExecutionState = {
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

object JobExecutionState {
  def apply(
      job: LJob, 
      status: JobStatus = JobStatus.NotStarted, 
      runCount: Int = 0): JobExecutionState = new JobExecutionState(job, encode(status), runCount.toByte)
  
  private val jobStatusTable: Array[JobStatus] = {
    val n = JobStatus.values.size
    
    require(n <= Byte.MaxValue)
    
    val table = Array.ofDim[JobStatus](n)
    
    val names = JobStatus.values.toSeq.sortBy(_.name)
    
    names.zipWithIndex.foreach { 
      case (jobStatus, i) => table(i) = jobStatus
    }
    
    table
  }
  
  private def encode(jobStatus: JobStatus): Byte = jobStatusTable.indexOf(jobStatus).toByte
  private def decode(encoded: Byte): JobStatus = jobStatusTable(encoded)
  
  def initialFor(job: LJob): JobExecutionState = apply(job)
}
