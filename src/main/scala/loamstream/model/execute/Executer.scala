package loamstream.model.execute

import scala.concurrent.duration.Duration

import ExecuterHelpers.toShotMap
import loamstream.model.jobs.JobState
import loamstream.model.jobs.LJob
import loamstream.util.Shot

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
trait Executer {

  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[JobState]]
  
}
