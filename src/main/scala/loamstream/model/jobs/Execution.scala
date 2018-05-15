package loamstream.model.jobs

import loamstream.model.execute.Environment
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import loamstream.model.jobs.commandline.CommandLineJob
import java.nio.file.Path
import loamstream.util.Loggable
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.Resources.LsfResources

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(
    env: Environment,
    cmd: Option[String] = None,
    status: JobStatus,
    result: Option[JobResult] = None,
    resources: Option[Resources] = None,
    outputs: Set[OutputRecord] = Set.empty,
    outputStreams: Option[OutputStreams]) {

  require(
      environmentAndResourcesMatch, 
      s"Environment type and resources must match, but got ${env.tpe} and $resources")
  
  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure

  def settings: Settings = env.settings
  
  private def environmentAndResourcesMatch: Boolean = (env.tpe, resources) match {
    case (_, None) => true
    case (EnvironmentType.Local, Some(_: LocalResources)) => true
    case (EnvironmentType.Google, Some(_: GoogleResources)) => true
    case (EnvironmentType.Uger, Some(_: UgerResources)) => true
    case (EnvironmentType.Lsf, Some(_: LsfResources)) => true
    case _ => false
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
  
  def withStatusAndResult(status: JobStatus, result: JobResult): Execution = {
    copy(status = status, result = Option(result))
  }
}

//TODO: Clean up and consolidate factory methods.  We probably don't need so many.  Maybe name them better too.
object Execution extends Loggable {

  // TODO Remove when dynamic statuses flow in
  // What does this mean? -Clint Dec 2017
  def apply(env: Environment,
            cmd: String,
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        env = env, 
        cmd = Option(cmd), 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet,
        outputStreams = Option(outputStreams))
  }

  def apply(env: Environment,
            cmd: String,
            status: JobStatus,
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: OutputRecord*): Execution = {
    
    Execution(
        env = env, 
        cmd = Option(cmd), 
        status = status, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet,
        outputStreams = Option(outputStreams))
  }

  def fromOutputs(env: Environment,
                  cmd: String,
                  result: JobResult,
                  outputStreams: OutputStreams,
                  outputs: Set[Output]): Execution = {
    
    apply(
        env = env, 
        cmd = Option(cmd), 
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.map(_.toOutputRecord),
        outputStreams = Option(outputStreams))
  }

  def from(job: LJob, jobStatus: JobStatus, outputStreams: OutputStreams): Execution = {
    from(job, jobStatus, result = None, Option(outputStreams))
  }
  
  def from(job: LJob, jobStatus: JobStatus): Execution = {
    from(job, jobStatus, result = None, outputStreams = None)
  }

  def from(
      job: LJob, 
      status: JobStatus, 
      result: Option[JobResult], 
      outputStreams: Option[OutputStreams]): Execution = {
    
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }
    
    val outputRecords = job.outputs.map(_.toOutputRecord)
    
    // TODO Replace the placeholder for `resources` object put in place to get the code to compile
    Execution(
      env = job.executionEnvironment,
      cmd = commandLine,
      status = status,
      result = result,
      resources = None, // TODO: smell: we have no idea how this job was run
      outputs = outputRecords,
      outputStreams = outputStreams) 
  }
}
