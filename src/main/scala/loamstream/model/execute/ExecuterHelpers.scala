package loamstream.model.execute


import scala.concurrent.{ExecutionContext, Future}
import loamstream.model.jobs.{Execution, LJob}
import loamstream.model.jobs.JobStatus
import loamstream.util.Futures
import loamstream.util.Loggable

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers extends Loggable {
  def noFailures[J <: LJob](m: Map[J, Execution]): Boolean = m.values.forall(_.status.isSuccess)
  
  def anyFailures[J <: LJob](m: Map[J, Execution]): Boolean = !noFailures(m)

  def executeSingle(job: LJob, shouldRestart: LJob => Boolean)(implicit executor: ExecutionContext): Future[(LJob, Execution)] = {
    job.transitionTo(JobStatus.NotStarted)
    job.transitionTo(JobStatus.Running)
    
    val result = for {
      execution <- job.execute
    } yield {
      job -> execution
    }

    import Futures.Implicits._
  
    //TODO TEST
    result.withSideEffect(handleResultOfExecution(shouldRestart))
  }
  
  //TODO TEST
  private[execute] def handleResultOfExecution(shouldRestart: LJob => Boolean)(tuple: (LJob, Execution)): Unit = {
    val (job, execution) = tuple
    
    trace(s"Handling result of execution: $job => $execution")
    
    val newStatus = execution.status
    
    if(newStatus.isFailure) {
      handleFailure(shouldRestart, newStatus)(job)
    } else {
      job.transitionTo(newStatus)
    }
  }
  
  //TODO: TEST
  private[execute] def determineFinalStatus(
      shouldRestart: LJob => Boolean, 
      failureStatus: JobStatus, 
      job: LJob): JobStatus = if(shouldRestart(job)) failureStatus else JobStatus.FailedPermanently
  
  //TODO: TEST, 
  //TODO: Refactor; this signature is convenient for UgerChunkRunner, but weird for anything else.  
  //TODO: Seperate computation of final status from side effect of munging job
  def handleFailure(shouldRestart: LJob => Boolean, failureStatus: JobStatus)(job: LJob): Unit = {
    
    val status = determineFinalStatus(shouldRestart, failureStatus, job)
    
    debug(s"$job transitioning to: $status (Non-terminal failure status: $failureStatus)")
    
    job.transitionTo(status)
  }
  
  def flattenTree(roots: Set[LJob]): Set[LJob] = {
    roots.foldLeft(roots) { (acc, job) =>
      job.inputs ++ flattenTree(job.inputs) ++ acc
    }
  }
}
