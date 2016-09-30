package loamstream.model.jobs

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class Execution(exitState: JobState, outputs: Set[Output]) {
  def transformOutputs(f: Set[Output] => Set[Output]): Execution = copy(outputs = f(outputs))
  
  def isCommandExecution: Boolean = exitState match {
    case JobState.CommandResult(_) => true
    case _ => false
  }
}