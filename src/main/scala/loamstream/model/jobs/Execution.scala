package loamstream.model.jobs

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
import loamstream.model.jobs.JobResult.CommandResult

/**
 * @author clint
 *         kyuksel
 * date: Sep 22, 2016
 */
final case class Execution(
    cmd: Option[String] = None,
    settings: Settings,
    status: JobStatus,
    result: Option[JobResult] = None,
    resources: Option[Resources] = None,
    outputs: Set[StoreRecord] = Set.empty,
    outputStreams: Option[OutputStreams],
    terminationReason: Option[TerminationReason]) {

  require(
      environmentAndResourcesMatch, 
      s"Environment type and resources must match, but got ${settings.envType} and $resources")
  
  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure
  
  private def environmentAndResourcesMatch: Boolean = (settings.envType, resources) match {
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

  def withStoreRecords(newOutputs: Set[StoreRecord]): Execution = copy(outputs = newOutputs)
  
  def withStoreRecords(newOutput: StoreRecord, others: StoreRecord*): Execution = {
    withStoreRecords((newOutput +: others).toSet)
  }

  def withResources(rs: Resources): Execution = copy(resources = Some(rs))
  
  def withStatusAndResult(status: JobStatus, result: JobResult): Execution = {
    copy(status = status, result = Option(result))
  }
}

//TODO: Clean up and consolidate factory methods.  We probably don't need so many.  Maybe name them better too.
object Execution extends Loggable {

  object WithCommandResult {
    def unapply(e: Execution): Option[CommandResult] = e.result match {
      case Some(cr: CommandResult) => Some(cr)
      case _ => None
    }
  }
  
  object WithCommandInvocationFailure {
    def unapply(e: Execution): Option[JobResult.CommandInvocationFailure] = e.result match {
      case Some(cif: JobResult.CommandInvocationFailure) => Some(cif)
      case _ => None
    }
  }
  
  // TODO Remove when dynamic statuses flow in
  // What does this mean? -Clint Dec 2017
  def apply(settings: Settings,
            cmd: String,
            result: JobResult,
            outputStreams: OutputStreams,
            outputs: StoreRecord*): Execution = {
    
    Execution(
        cmd = Option(cmd),
        settings = settings,
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.toSet,
        outputStreams = Option(outputStreams),
        terminationReason = None)
  }

  def fromOutputs(settings: Settings,
                  cmd: String,
                  result: JobResult,
                  outputStreams: OutputStreams,
                  outputs: Set[DataHandle]): Execution = {
    
    apply(
        cmd = Option(cmd),
        settings = settings,
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs.map(_.toStoreRecord),
        outputStreams = Option(outputStreams),
        terminationReason = None)
  }

  def from(
      job: LJob, 
      status: JobStatus, 
      result: Option[JobResult] = None, 
      outputStreams: Option[OutputStreams] = None,
      resources: Option[Resources] = None,
      terminationReason: Option[TerminationReason]): Execution = {
    
    val commandLine: Option[String] = job match {
      case clj: CommandLineJob => Option(clj.commandLineString)
      case _ => None
    }
    
    val outputRecords = job.outputs.map(_.toStoreRecord)
    
    Execution(
      settings = job.initialSettings,
      cmd = commandLine,
      status = status,
      result = result,
      resources = resources, 
      outputs = outputRecords,
      outputStreams = outputStreams,
      terminationReason = terminationReason)
  }
}
