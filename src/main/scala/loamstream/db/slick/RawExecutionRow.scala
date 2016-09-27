package loamstream.db.slick

import loamstream.model.jobs.Execution
import loamstream.model.jobs.Output

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class RawExecutionRow(id: Int, exitStatus: Int) {
  def toExecution(outputs: Set[Output]): Execution = Execution(exitStatus, outputs)
}