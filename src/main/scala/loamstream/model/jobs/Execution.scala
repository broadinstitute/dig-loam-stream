package loamstream.model.jobs

import loamstream.conf.LocalSettings
import loamstream.conf.Settings
import loamstream.model.execute.Environment
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.EnvironmentType
import loamstream.conf.UgerSettings
import loamstream.conf.GoogleSettings

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(
    id: Option[Int] = None,
    //TODO: Consolidate with `settings` into an Environment
    envType: EnvironmentType,
    cmd: Option[String] = None,
    settings: Settings,
    status: JobStatus,
    result: Option[JobResult] = None,
    resources: Option[Resources] = None,
    outputs: Set[OutputRecord] = Set.empty) {

  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  def env: Environment = (envType, settings) match {
    case (EnvironmentType.Local, _) => Environment.Local
    case (EnvironmentType.Google, googleSettings: GoogleSettings) => Environment.Google(googleSettings)
    case (EnvironmentType.Uger, ugerSettings: UgerSettings) => Environment.Uger(ugerSettings)
    case _ => ???
  }
  
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
  def apply(env: EnvironmentType,
            cmd: Option[String],
            settings: Settings,
            result: JobResult,
            outputs: Set[OutputRecord]): Execution = {
    
    Execution(id = None, env, cmd, settings, result.toJobStatus, Some(result), None, outputs)
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: EnvironmentType,
            cmd: String,
            settings: Settings,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    
    Execution(id = None, env, Option(cmd), settings, result.toJobStatus, Some(result), None, outputs.toSet)
  }

  def apply(env: EnvironmentType,
            cmd: String,
            settings: Settings,
            status: JobStatus,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    
    Execution(id = None, env, Option(cmd), settings, status, Some(result), None, outputs.toSet)
  }

  def fromOutputs(env: EnvironmentType,
                  cmd: String,
                  settings: Settings,
                  result: JobResult,
                  outputs: Set[Output]): Execution = {
    
    Execution(env, Option(cmd), settings, result, outputs.map(_.toOutputRecord))
  }

  def from(job: LJob, jobStatus: JobStatus): Execution = from(job, jobStatus, result = None)

  def from(job: LJob, status: JobStatus, result: Option[JobResult]): Execution = {
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }

    // TODO Replace the placeholders for `settings` and `resourcess` objects 
    // put in place to get the code to compile
    Execution(
      id = None,
      envType = job.executionEnvironment.tpe,
      cmd = commandLine,
      settings = job.executionEnvironment.settings, // TODO: smell: we have no idea how this job was run
      status = status,
      result = result,
      resources = None, // TODO: smell: we have no idea how this job was run
      outputs = job.outputs.map(_.toOutputRecord)) 
  }
}
