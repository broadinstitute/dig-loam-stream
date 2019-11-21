package loamstream.model.execute

import scala.concurrent.duration.Duration

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob
import loamstream.util.Terminable

/**
 * @author oliverr
 *         date: 2/24/16
 */
trait Executer extends Terminable {

  def jobFilter: JobFilter 
  
  def execute(
      executable: Executable, 
      makeJobOracle: Executable => JobOracle)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution]
  
  override def stop(): Iterable[Throwable] = Nil
  
}
