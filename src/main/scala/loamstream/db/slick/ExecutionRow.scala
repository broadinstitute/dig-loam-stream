package loamstream.db.slick

import loamstream.conf.Settings
import loamstream.model.execute.Environment
import loamstream.model.execute.Resources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.OutputRecord
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.execute.EnvironmentType

/**
 * @author clint
 *         date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, env: String, cmd: String, status: JobStatus, exitCode: Int) {
  def toExecution(settings: Settings, resourcesOpt: Option[Resources], outputs: Set[OutputRecord]): Execution = {
    val commandResult = CommandResult(exitCode)

    val environmentOpt: Option[Environment] = for {
      tpe <- EnvironmentType.fromString(env)
      e <- Environment.from(tpe, settings)
    } yield e
    
    //TODO: :(
    require(environmentOpt.isDefined)
    
    Execution(
        id = Option(mapId(identity)),
        env = environmentOpt.get,
        cmd = Option(cmd),
        status = status,
        result = Option(commandResult),
        resources = resourcesOpt,
        outputs = outputs)
  }

  /**
   * Meant to abstract out Execution.id from ExecutionRow.id
   * to prevent database-level information from leaking up
   */
  //TODO: Why is this here?
  private def mapId[A](f: Int => A): A = f(id)
}
