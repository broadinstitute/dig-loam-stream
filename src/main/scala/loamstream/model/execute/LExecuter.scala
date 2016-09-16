package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.Shot

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
trait LExecuter {
  
  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]]
}
