package loamstream.model.jobs

import loamstream.model.execute.{ExecutionEnvironment, Resources, Settings}
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.LocalSettings

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(env: ExecutionEnvironment,
                           cmd: Option[String],
                           settings: Settings,
                           exitState: JobState,
                           outputs: Set[OutputRecord]) {

  def isSuccess: Boolean = exitState.isSuccess
  def isFailure: Boolean = exitState.isFailure

  def transformOutputs(f: Set[OutputRecord] => Set[OutputRecord]): Execution = copy(outputs = f(outputs))

  //NB :(
  //We're a command execution if we wrap a CommandResult or CommandInvocationFailure, and a 
  //command-line string is defined. 
  def isCommandExecution: Boolean = exitState match {
    case _: JobState.CommandResult | _: JobState.CommandInvocationFailure => cmd.isDefined
    case _ => false
  }

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution = {
    withOutputRecords((newOutput +: others).toSet)
  }
  
  def resources: Option[Resources] = exitState.resources
}

object Execution {
  def apply(env: ExecutionEnvironment,
            cmd: String,
            settings: Settings,
            exitState: JobState,
            outputs: OutputRecord*): Execution = {
    Execution(env, Option(cmd), settings, exitState, outputs.toSet)
  }

  def fromOutputs(env: ExecutionEnvironment,
                  cmd: String,
                  settings: Settings,
                  exitState: JobState,
                  outputs: Set[Output]): Execution = {
    Execution(env, Option(cmd), settings, exitState, outputs.map(_.toOutputRecord))
  }

  def fromOutputs(env: ExecutionEnvironment,
                  cmd: String,
                  settings: Settings,
                  exitState: JobState,
                  output: Output,
                  others: Output*): Execution = {
    fromOutputs(env, cmd, settings, exitState, (output +: others).toSet)
  }
  
  def from(job: LJob, jobState: JobState): Execution = {
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }
    
    // TODO Replace the placeholders for `settings` objects put in place to get the code to compile
    Execution(
      job.executionEnvironment, 
      commandLine, 
      LocalSettings(), // TODO
      jobState, 
      job.outputs.map(_.toOutputRecord)) 
  }
}
