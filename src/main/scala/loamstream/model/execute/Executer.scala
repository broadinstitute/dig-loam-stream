package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.model.jobs.{Execution, LJob}
import loamstream.util.Terminable

/**
 * @author oliverr
 *         date: 2/24/16
 */
trait Executer extends Terminable {

  def jobFilter: JobFilter 
  
  def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution]
  
  override def stop(): Unit = ()
  
}
