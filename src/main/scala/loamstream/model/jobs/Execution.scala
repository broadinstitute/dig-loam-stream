package loamstream.model.jobs

import loamstream.model.execute.Settings

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(settings: Settings, exitState: JobState, outputs: Set[OutputRecord]) {
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
  def apply(settings: Settings, exitState: JobState, outputs: OutputRecord*): Execution =
    Execution(settings, exitState, outputs.toSet)

  def fromOutputs(settings: Settings, exitState: JobState, outputs: Set[Output]): Execution =
    Execution(settings, exitState, outputs.map(_.toOutputRecord))
  def fromOutputs(settings: Settings, exitState: JobState, output: Output, others: Output*): Execution =
    fromOutputs(settings, exitState, (output +: others).toSet)
}
