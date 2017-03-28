package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.model.jobs.{Execution, LJob}

/**
 * @author oliverr
 *         date: 2/24/16
 */
trait Executer {

  def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution]
  
}
