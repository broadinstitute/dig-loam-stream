package loamstream.db.slick

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}
import loamstream.model.jobs.{Execution, OutputRecord}
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.execute.Resources.LocalResources

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, env: String, exitStatus: Int) {
  def toExecution(settings: Settings, resources: Resources, outputs: Set[OutputRecord]): Execution = {
    //TODO: `LocalResources` is just a dummy transitional value
    val commandResult = CommandResult(exitStatus, Option(LocalResources.DUMMY))
    
    Execution(ExecutionEnvironment.fromString(env), settings, resources, commandResult, outputs)
  }
}
