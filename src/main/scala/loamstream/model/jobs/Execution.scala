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
                           status: JobStatus,
                           result: JobResult,
                           outputs: Set[OutputRecord]) {

  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  def transformOutputs(f: Set[OutputRecord] => Set[OutputRecord]): Execution = copy(outputs = f(outputs))

  //NB :(
  //We're a command execution if we wrap a CommandResult or CommandInvocationFailure, and a
  //command-line string is defined.
  def isCommandExecution: Boolean = result match {
    case _: JobResult.CommandResult | _: JobResult.CommandInvocationFailure => cmd.isDefined
    case _ => false
  }

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution = {
    withOutputRecords((newOutput +: others).toSet)
  }

  def resources: Option[Resources] = result.resources
}

object Execution {
  // TODO Remove when dynamic statuses flow in
  def apply(env: ExecutionEnvironment,
            cmd: Option[String],
            settings: Settings,
            result: JobResult,
            outputs: Set[OutputRecord]): Execution = {
    Execution(env, cmd, settings, JobStatus.Unknown, result, outputs)
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: ExecutionEnvironment,
            cmd: Option[String],
            settings: Settings,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    Execution(env, cmd, settings, JobStatus.Unknown, result, outputs.toSet)
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: ExecutionEnvironment,
            cmd: String,
            settings: Settings,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    Execution(env, Option(cmd), settings, JobStatus.Unknown, result, outputs.toSet)
  }

  def apply(env: ExecutionEnvironment,
            cmd: String,
            settings: Settings,
            status: JobStatus,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    Execution(env, Option(cmd), settings, status, result, outputs.toSet)
  }

  def fromOutputs(env: ExecutionEnvironment,
                  cmd: String,
                  settings: Settings,
                  result: JobResult,
                  outputs: Set[Output]): Execution = {
    Execution(env, Option(cmd), settings, result, outputs.map(_.toOutputRecord))
  }

  def fromOutputs(env: ExecutionEnvironment,
                  cmd: String,
                  settings: Settings,
                  result: JobResult,
                  output: Output,
                  others: Output*): Execution = {
    fromOutputs(env, cmd, settings, result, (output +: others).toSet)
  }

  def from(job: LJob, jobStatus: JobStatus): Execution = {
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }

    // TODO Replace the placeholders for `settings` objects put in place to get the code to compile
    Execution(
      job.executionEnvironment,
      commandLine,
      LocalSettings(), // TODO
      jobStatus,
      JobResult.NoResult, // TODO
      job.outputs.map(_.toOutputRecord)) 
  }
}
