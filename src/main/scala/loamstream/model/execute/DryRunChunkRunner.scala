package loamstream.model.execute

import loamstream.model.jobs.LJob
import rx.lang.scala.Observable
import loamstream.model.jobs.RunData
import loamstream.util.Maps
import loamstream.util.Traversables
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.JobResult

/**
 * @author clint
 * Feb 12, 2018
 */
final class DryRunChunkRunner extends ChunkRunner {
  override def maxNumJobs: Int = Int.MaxValue //TODO
  
  override def canRun(job: LJob): Boolean = true
  
  override def run(jobs: Set[LJob], shouldRestart: LJob => Boolean): Observable[Map[LJob, RunData]] = {
    import Traversables.Implicits._
    
    val successStatus: JobStatus = JobStatus.Succeeded
    
    val jobsToRuns = jobs.mapTo { job =>
      RunData(
          job = job,
          jobStatus = successStatus,
          jobResult = Some(JobResult.Success),
          resourcesOpt = None,
          outputStreamsOpt = None)
    }
    
    jobs.foreach(_.transitionTo(successStatus))
    
    Observable.just(jobsToRuns)
  }
}
