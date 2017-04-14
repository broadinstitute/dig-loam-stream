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

  def executeSingle(
      job: LJob, 
      shouldRestart: LJob => Boolean)(implicit executor: ExecutionContext): Future[(LJob, Execution)] = {
    
    job.transitionTo(JobStatus.NotStarted)
    job.transitionTo(JobStatus.Running)
    
    val result = for {
      execution <- job.execute
    } yield {
      job -> execution
    }

    import Futures.Implicits._
  
    result.withSideEffect(handleResultOfExecution(shouldRestart))
  }
  
  private[execute] def handleResultOfExecution(shouldRestart: LJob => Boolean)(tuple: (LJob, Execution)): Unit = {
    val (job, execution) = tuple
    
    trace(s"Handling result of execution: $job => $execution")
    
    val newStatus = determineFinalStatus(shouldRestart, execution.status, job)
    
    job.transitionTo(newStatus)
  }
  
  private[execute] def determineFinalStatus(
      shouldRestart: LJob => Boolean,
      newStatus: JobStatus,
      job: LJob): JobStatus = {
    
    if(newStatus.isFailure) determineFailureStatus(shouldRestart, newStatus, job) else newStatus
  }
  
  def determineFailureStatus(
      shouldRestart: LJob => Boolean, 
      failureStatus: JobStatus, 
      job: LJob): JobStatus = {
    
    val restarting = shouldRestart(job)
    
    if(restarting) {
      info(s"Restarting job $job")
      
      failureStatus
    } else {
       JobStatus.FailedPermanently
    }
  }
  
  def flattenTree(roots: Set[LJob]): Set[LJob] = {
    roots.foldLeft(roots) { (acc, job) =>
      job.inputs ++ flattenTree(job.inputs) ++ acc
    }
  }
}
