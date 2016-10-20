package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
trait Executer {

  def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, JobState]
  
}
