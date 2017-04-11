package loamstream.model.execute


import scala.concurrent.{ExecutionContext, Future}
import loamstream.model.jobs.{Execution, LJob}
import loamstream.model.jobs.JobStatus
import loamstream.util.Futures

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers {
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
  private def handleResultOfExecution(shouldRestart: LJob => Boolean)(tuple: (LJob, Execution)): Unit = {
    val (job, execution) = tuple
    
    println(s"EXECUTERHELPERS.handleResultOfExecution: $job => $execution")
    
    if(execution.status.isFailure) {
      handleFailure(shouldRestart)(job)
    } else {
      job.transitionTo(execution.status)
    }
  }
  
  //TODO: TEST
  def handleFailure(shouldRestart: LJob => Boolean)(job: LJob): Unit = {
    job.transitionTo(JobStatus.Failed)
    
    val status = if(shouldRestart(job)) JobStatus.NotStarted else JobStatus.PermanentFailure
    
    println(s"EXECUTERHELPERS.handleFailure: $job transitioning to: $status")
    
    job.transitionTo(status)
  }
  
  def flattenTree(roots: Set[LJob]): Set[LJob] = {
    roots.foldLeft(roots) { (acc, job) =>
      job.inputs ++ flattenTree(job.inputs) ++ acc
    }
  }
}
