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

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution =
    withOutputRecords((newOutput +: others).toSet)
}

object Execution {
  def fromOutputs(exitState: JobState, outputs: Set[Output]): Execution =
    Execution(exitState, outputs.map(_.toOutputRecord))
  def fromOutputs(exitState: JobState, output: Output, others: Output*): Execution =
    fromOutputs(exitState, (output +: others).toSet)
}