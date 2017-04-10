package loamstream.db.slick

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}
import loamstream.model.jobs.{Execution, JobStatus, OutputRecord}
import loamstream.model.jobs.JobResult.CommandResult

/**
 * @author clint
 *         date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, env: String, cmd: String, status: JobStatus, exitCode: Int) {
  def toExecution(settings: Settings, resourcesOpt: Option[Resources], outputs: Set[OutputRecord]): Execution = {
    val commandResult = CommandResult(exitCode)

    Execution(Option(mapId(identity)),
              ExecutionEnvironment.fromString(env),
              Option(cmd),
              settings,
              status,
              Some(commandResult),
              resourcesOpt,
              outputs)
  }

  /**
   * Meant to abstract out Execution.id from ExecutionRow.id
   * to prevent database-level information from leaking up
   */
  private def mapId[A](f: Int => A): A = f(id)
}
