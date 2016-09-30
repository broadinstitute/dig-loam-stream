package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import ExecuterHelpers.toShotMap

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.util.Maps
import loamstream.util.Shot
import loamstream.model.jobs.JobState

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
trait Executer {

  def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[JobState]]
  
}
