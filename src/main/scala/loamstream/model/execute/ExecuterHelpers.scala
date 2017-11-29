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
      val inputJobs = job.inputs.map(_.job)
      
      inputJobs ++ flattenTree(inputJobs) ++ acc
    }
  }
}
