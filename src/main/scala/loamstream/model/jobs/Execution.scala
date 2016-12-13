package loamstream.model.jobs

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class Execution(exitState: JobState, outputs: Set[OutputRecord]) {
  def isSuccess: Boolean = exitState.isSuccess
  def isFailure: Boolean = exitState.isFailure

  def transformOutputs(f: Set[OutputRecord] => Set[OutputRecord]): Execution = copy(outputs = f(outputs))

  //NB :(
  def isCommandExecution: Boolean = exitState match {
    case JobState.CommandResult(_) | JobState.CommandInvocationFailure(_) => true
    case _ => false
  }

  def withOutputs(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
}