package loamstream.db.slick

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}
import loamstream.model.jobs.{Execution, OutputRecord}
import loamstream.model.jobs.JobState.CommandResult

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, env: String, exitStatus: Int) {
  def toExecution(settings: Settings, resources: Resources, outputs: Set[OutputRecord]): Execution =
    Execution(ExecutionEnvironment.fromString(env), settings, resources, CommandResult(exitStatus), outputs)
}
