package loamstream.model.jobs

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(env: ExecutionEnvironment,
                           settings: Settings,
                           resources: Resources,
                           exitState: JobState,
                           outputs: Set[OutputRecord]) {

  def isSuccess: Boolean = exitState.isSuccess
  def isFailure: Boolean = exitState.isFailure

  def transformOutputs(f: Set[OutputRecord] => Set[OutputRecord]): Execution = copy(outputs = f(outputs))

  //NB :(
  def isCommandExecution: Boolean = exitState match {
    case _: JobState.CommandResult | _: JobState.CommandInvocationFailure => true
    case _ => false
  }

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution =
    withOutputRecords((newOutput +: others).toSet)
}

object Execution {
  def apply(env: ExecutionEnvironment,
            settings: Settings,
            resources: Resources,
            exitState: JobState,
            outputs: OutputRecord*): Execution =
    Execution(env, settings, resources, exitState, outputs.toSet)

  def fromOutputs(env: ExecutionEnvironment,
                  settings: Settings,
                  resources: Resources,
                  exitState: JobState,
                  outputs: Set[Output]): Execution =
    Execution(env, settings, resources, exitState, outputs.map(_.toOutputRecord))

  def fromOutputs(env: ExecutionEnvironment,
                  settings: Settings,
                  resources: Resources,
                  exitState: JobState,
                  output: Output,
                  others: Output*): Execution =
    fromOutputs(env, settings, resources, exitState, (output +: others).toSet)
}
