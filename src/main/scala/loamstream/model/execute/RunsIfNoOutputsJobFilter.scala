package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Loggable

/**
 * @author clint
 * Nov 14, 2018
 */
object RunsIfNoOutputsJobFilter extends JobFilter with Loggable {
  override def shouldRun(job: LJob): Boolean = {
    val result = job.outputs.isEmpty
    
    if (result) {
      debug(s"Job $job will be run because it has no known outputs") 
    }
    
    result
  }
}
