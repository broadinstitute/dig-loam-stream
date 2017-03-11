package loamstream.db.slick

import loamstream.model.jobs.{Execution, Output, OutputRecord}
import loamstream.model.jobs.JobState.CommandResult
import loamstream.oracle.Resources.LocalResources

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class ExecutionRow(id: Int, exitStatus: Int) {
  def toExecution(outputs: Set[OutputRecord]): Execution = {
    //TODO: `LocalResources` is just a dummy transitional value
    Execution(CommandResult(exitStatus, LocalResources), outputs)
  }
  
}
