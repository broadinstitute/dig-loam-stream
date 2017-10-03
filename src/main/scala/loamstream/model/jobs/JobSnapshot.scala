package loamstream.model.jobs

/**
 * @author clint
 * Apr 12, 2017
 * 
 * A class to represent the mutable fields in an LJob.  Allows changing one or both atomically.
 * 
 * NB: I would have named this `JobState`, but that seemed too similar to `JobStatus`. 
 */
final case class JobSnapshot(status: JobStatus, runCount: Int) {
  def transitionTo(newStatus: JobStatus): JobSnapshot = {
    val startedRunning = newStatus.isRunning && this.status.notRunning
      
    val newRunCount = if(startedRunning) runCount + 1 else runCount
      
    JobSnapshot(newStatus, newRunCount)
  }
  
  def withStatus(newStatus: JobStatus): JobSnapshot = copy(status = newStatus)
}
