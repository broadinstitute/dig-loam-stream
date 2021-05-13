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
import loamstream.model.execute.Resources.AwsResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobResult.FailureWithException
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.GoogleSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.model.execute.LsfDrmSettings
import loamstream.model.execute.AwsSettings
import scala.collection.compat._

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
    outputs: Iterable[StoreRecord] = Seq.empty,
    jobDir: Option[Path],
    terminationReason: Option[TerminationReason]) extends Execution.Persisted {

  require(
      environmentAndResourcesMatch, 
      s"Environment type and resources must match, but got $envType and $resources")
      
  override def envType: EnvironmentType = settings.envType
      
  def isSuccess: Boolean = status.isSuccess
  def isFailure: Boolean = status.isFailure
  def isSkipped: Boolean = status.isSkipped
  
  private def environmentAndResourcesMatch: Boolean = (envType, resources) match {
    case (_, None) => true
    case (EnvironmentType.Local, Some(_: LocalResources)) => true
    case (EnvironmentType.Google, Some(_: GoogleResources)) => true
    case (EnvironmentType.Uger, Some(_: UgerResources)) => true
    case (EnvironmentType.Lsf, Some(_: LsfResources)) => true
    case (EnvironmentType.Aws, Some(_: AwsResources)) => true
    case _ => false
  }

  def isPersistable: Boolean = isSkipped || cmd.isDefined
  def notPersistable: Boolean = !isPersistable
  
  //NB :(
  //We're a command execution if we wrap a CommandResult or CommandInvocationFailure, and a
  //command-line string is defined.
  private def isCommandExecution: Boolean = result.exists {
    case _: JobResult.CommandResult | _: JobResult.CommandInvocationFailure => true
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

  trait Persisted {
    def envType: EnvironmentType
    def cmd: Option[String]
    def status: JobStatus
    def result: Option[JobResult]
    def outputs: Iterable[StoreRecord]
    def jobDir: Option[Path]
    def terminationReason: Option[TerminationReason]
  }
  
  object WithCommandResult {
    def unapply(e: Execution): Option[CommandResult] = e.status match {
      case JobStatus.Skipped => Some(CommandResult(0))
      case _ => e.result.collect { case cr: CommandResult => cr }
    }
  }
  
  object WithCommandInvocationFailure {
    def unapply(e: Execution): Option[JobResult.CommandInvocationFailure] = e.result.collect {
      case cif: JobResult.CommandInvocationFailure => cif
    }
  }
  
  object WithThrowable {
    def unapply(e: Execution): Option[Throwable] = e.result match {
      case Some(JobResult.CommandInvocationFailure(e)) => Some(e)
      case Some(JobResult.FailureWithException(e)) => Some(e)
      case _ => None
    }
  }
  
  // TODO Remove when dynamic statuses flow in
  // What does this mean? -Clint Dec 2017
  def apply(settings: Settings,
            cmd: String,
            result: JobResult,
            jobDir: Path,
            outputs: StoreRecord*): Execution = {
    
    Execution(
        cmd = Option(cmd),
        settings = settings,
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = outputs,
        jobDir = Option(jobDir),
        terminationReason = None)
  }

  def fromOutputs(settings: Settings,
                  cmd: String,
                  result: JobResult,
                  jobDir: Path,
                  outputs: Iterable[DataHandle]): Execution = {
    
    Execution(
        cmd = Option(cmd),
        settings = settings,
        status = result.toJobStatus, 
        result = Option(result), 
        resources = None, 
        outputs = toStoreRecords(outputs),
        jobDir = Option(jobDir),
        terminationReason = None)
  }

  def from(
      job: LJob, 
      status: JobStatus, 
      result: Option[JobResult] = None, 
      jobDir: Option[Path] = None,
      resources: Option[Resources] = None,
      terminationReason: Option[TerminationReason] = None): Execution = {
    
    val commandLine: Option[String] = Identifier.from(job)
    
    val outputRecords = toStoreRecords(job.outputs)
    
    val settings = job.initialSettings
    
    Execution(
      settings = settings,
      cmd = commandLine,
      status = status,
      result = result,
      resources = resources, 
      outputs = outputRecords,
      jobDir = jobDir,
      terminationReason = terminationReason)
  }
  
  private def toStoreRecords(dataHandles: Iterable[DataHandle]): Iterable[StoreRecord] = {
    //Note .to(Seq) here.  This prevents building a set of StoreRecords (as long as job.outputs is a Set),
    //which would invoke StoreRecord.equals for each StoreRecord produced, forcing the evaluation of thos
    //StoreRecords' hash fields.  We want to prevent forcing the evaluation of those fields and leave the
    //the decision to hash or not to ExecutionRecorders. (Ie, don't eval hashes if they won't be used.) 
    dataHandles.to(Seq).map(_.toStoreRecord)
  }
}
