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
    env: Environment,
    cmd: Option[String] = None,
    status: JobStatus,
    result: Option[JobResult] = None,
    resources: Option[Resources] = None,
    outputs: Set[OutputRecord] = Set.empty) {

  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  def settings: Settings = env.settings
  
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
  def apply(env: Environment,
            cmd: Option[String],
            result: JobResult,
            outputs: Set[OutputRecord]): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = cmd, 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs)
  }

  // TODO Remove when dynamic statuses flow in
  def apply(env: Environment,
            cmd: String,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = Option(cmd), 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet)
  }

  def apply(env: Environment,
            cmd: String,
            status: JobStatus,
            result: JobResult,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        id = None, 
        env = env, 
        cmd = Option(cmd), 
        status = status, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet)
  }

  def fromOutputs(env: Environment,
                  cmd: String,
                  result: JobResult,
                  outputs: Set[Output]): Execution = {
    
    apply(env, Option(cmd), result, outputs.map(_.toOutputRecord))
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
      env = job.executionEnvironment,
      cmd = commandLine,
      status = status,
      result = result,
      resources = None, // TODO: smell: we have no idea how this job was run
      outputs = job.outputs.map(_.toOutputRecord)) 
  }
}
