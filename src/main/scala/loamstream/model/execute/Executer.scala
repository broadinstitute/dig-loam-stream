package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.model.jobs.{Execution, LJob}
import rx.lang.scala.Observable

/**
 * @author oliverr
 *         date: 2/24/16
 */
trait Executer {

  def execute(jobs: Observable[LJob])(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution]
  
}
