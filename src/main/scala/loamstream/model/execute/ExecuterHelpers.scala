package loamstream.model.execute


import scala.concurrent.{ExecutionContext, Future}
import loamstream.model.jobs.{Execution, LJob}
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 * date: Jun 7, 2016
 */
object ExecuterHelpers {
  def noFailures[J <: LJob](m: Map[J, Execution]): Boolean = m.values.forall(_.status.isSuccess)
  
  def anyFailures[J <: LJob](m: Map[J, Execution]): Boolean = !noFailures(m)

  def executeSingle(job: LJob)(implicit executor: ExecutionContext): Future[(LJob, Execution)] = {
    job.transitionTo(JobStatus.NotStarted)
    job.transitionTo(JobStatus.Running)
    
    for {
      execution <- job.execute
    } yield {
      job.transitionTo(execution.status)
      
      job -> execution
    }
  }
  
  def flattenTree(roots: Set[LJob]): Set[LJob] = {
    roots.foldLeft(roots) { (acc, job) =>
      job.inputs ++ flattenTree(job.inputs) ++ acc
    }
  }
}
