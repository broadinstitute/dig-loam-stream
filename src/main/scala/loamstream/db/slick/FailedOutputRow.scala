package loamstream.db.slick

import loamstream.model.jobs.Output.PathOutput
import java.nio.file.Paths

/**
 * @author clint
 * Oct 14, 2016
 */
final case class FailedOutputRow(pathValue: String, executionId: Int) {
  def toPathOutput: PathOutput = PathOutput(Paths.get(pathValue)) 
  
  def withExecutionId(newExecutionId: Int): FailedOutputRow = copy(executionId = newExecutionId)
}