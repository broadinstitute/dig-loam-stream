package loamstream.model.jobs

import loamstream.model.execute.{ExecutionEnvironment, LocalSettings, Resources, Settings}
import loamstream.model.jobs.commandline.CommandLineJob

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(id: Option[Int] = None,
                           env: ExecutionEnvironment,
                           cmd: Option[String] = None,
                           settings: Settings,
                           status: JobStatus,
                           result: Option[JobResult] = None,
                           resources: Option[Resources] = None,
                           outputs: Set[OutputRecord] = Set.empty) {

  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  //NB :(
  //We're a command execution if we wrap a CommandResult or CommandInvocationFailure, and a
  //command-line string is defined.
  def isCommandExecution: Boolean = result.exists {
    case _: JobResult.CommandResult | _: JobResult.CommandInvocationFailure => cmd.isDefined
    case _ => false
  }

  def withOutputRecords(newOutputs: Set[OutputRecord]): Execution = copy(outputs = newOutputs)
  def withOutputRecords(newOutput: OutputRecord, others: OutputRecord*): Execution = {
    withOutputRecords((newOutput +: others).toSet)
  }

  def withResources(rs: Resources): Execution = copy(resources = Some(rs))

  def withId(newId: Int): Execution = withId(Option(newId))
  def withId(newId: Option[Int]): Execution = copy(id = newId)
}

object Execution {
  // TODO Remove when dynamic statuses flow in
  def apply(env: ExecutionEnvironment,
            cmd: Option[String],
            settings: Settings,
            result: JobResult,
            outputs: Set[OutputRecord]): Execution = {
    Execution(id = None, env, cmd, settings, result.toJobStatus, Some(result), None, outputs)
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: ExecutionEnvironment,
            cmd: String,
            settings: Settings,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    Execution(id = None, env, Option(cmd), settings, result.toJobStatus, Some(result), None, outputs.toSet)
  }

  def apply(env: ExecutionEnvironment,
            cmd: String,
            settings: Settings,
            status: JobStatus,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    Execution(id = None, env, Option(cmd), settings, status, Some(result), None, outputs.toSet)
  }

  def fromOutputs(env: ExecutionEnvironment,
                  cmd: String,
                  settings: Settings,
                  result: JobResult,
                  outputs: Set[Output]): Execution = {
    Execution(env, Option(cmd), settings, result, outputs.map(_.toOutputRecord))
  }

  def from(job: LJob, jobStatus: JobStatus): Execution = {
    from(job, jobStatus, result = None)
  }

  def from(job: LJob, status: JobStatus, result: Option[JobResult]): Execution = {
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }

    // TODO Replace the placeholders for `settings` objects put in place to get the code to compile
    Execution(
      id = None,
      job.executionEnvironment,
      commandLine,
      LocalSettings(), // TODO
      status,
      result,
      None,
      job.outputs.map(_.toOutputRecord)) 
  }
}
