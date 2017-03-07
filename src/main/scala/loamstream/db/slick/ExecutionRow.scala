package loamstream.db.slick

import loamstream.model.execute.Settings
import loamstream.model.jobs.{Execution, OutputRecord}
import loamstream.model.jobs.JobState.CommandResult

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, exitStatus: Int) {
  def toExecution(settings: Settings, outputs: Set[OutputRecord]): Execution = {
    Execution(settings, CommandResult(exitStatus), outputs)
  }
}
