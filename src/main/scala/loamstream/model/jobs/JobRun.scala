package loamstream.model.jobs

/**
 * @author clint
 * Apr 13, 2017
 * 
 * A class to represent a particular run of a job, now that jobs can potentially run multiple times.
 * This is needed since LJob is mutable, and we'd like to de-dupe streams of jobs that we're about to run.  
 * Instead, we work with and de-dupe `JobRuns`.  This makes for consistent equality checking (needed for 
 * de-duping) and allows distinguishing between the 1st, 2nd, ..., and Nth runs of a job, so those subsequent
 * runs are not removed by de-duping.  (See RxExecuter.execute, in particular, `runnables`.) 
 */
//NB: Not a case class so we can have a custom equals() method 
final class JobRun(val job: LJob, val status: JobStatus, val runCount: Int) {
  override def toString: String = s"${getClass.getSimpleName}($job,$status,$runCount)"
  
  override def equals(other: Any): Boolean = other match {
    //compare jobs by reference, status and runCount by 'value'
    case that: JobRun => (this.job eq that.job) && (this.status == that.status) && (this.runCount == that.runCount)
    case _ => false
  }
  
  override def hashCode: Int = Seq(job, status, runCount).hashCode
  
  def key: (Int, Int) = (job.id, runCount)
}

object JobRun {
  def apply(jobNode: JobNode, status: JobStatus, runCount: Int): JobRun = JobRun(jobNode.job, status, runCount)
  
  def apply(job: LJob, status: JobStatus, runCount: Int): JobRun = new JobRun(job, status, runCount)
  
  def unapply(jr: JobRun): Option[(LJob, JobStatus, Int)] = Some((jr.job, jr.status, jr.runCount))
}
