package loamstream.model.jobs

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import java.nio.file.Path
import loamstream.model.execute.Resources.LocalResources

/**
 * @author clint
 * Jan 3, 2018
 * 
 * Allows capturing info about the execution of a job without eagerly creating an Execution, which
 * would mean hashing any output files/uris. Deferring the creation of an Execution allows waiting to do so
 * until all outputs are present (needed to work around Uger/Broad FS issue where files created by jobs don't
 * become visible to the LS process until later.
 * 
 * TODO: A better name.  `JobRun` is taken, but perhaps *that* should be renamed.
 */
final case class RunData(
    job: LJob,
    settings: Settings,
    jobStatus: JobStatus, 
    jobResult: Option[JobResult],
    resourcesOpt: Option[Resources] = None, 
    jobDirOpt: Option[Path] = None,
    terminationReasonOpt: Option[TerminationReason]) {

  override def toString: String = {
    val name = getClass.getSimpleName
    val id = job.id
    
    s"${name}(Job#$id, $settings, $jobStatus, $jobResult, $resourcesOpt, $jobDirOpt, $terminationReasonOpt)"
  }
  
  def withSettings(s: Settings): RunData = copy(settings = s)
  
  def withResources(r: Resources): RunData = copy(resourcesOpt = Some(r))
  
  private[jobs] def cmdOpt: Option[String] = job match { 
    case clj: CommandLineJob => Some(clj.commandLineString)
    case _ => None
  }
  
  //NB: This is lazy, to allow waiting to hash outputs (done by `job.outputs.map(_.toOutputRecord)`) 
  //until they're ready, and to only do that once.
  lazy val toExecution: Execution = {
    val ultimateStatus = RunData.determineJobStatus(jobStatus)
    
    Execution(
      settings = settings,
      cmd = cmdOpt,
      status = ultimateStatus,
      result = jobResult,
      resources = resourcesOpt,
      outputs = job.outputs.map(_.toStoreRecord),
      jobDir = jobDirOpt,
      terminationReason = terminationReasonOpt)
  }
}

object RunData {
  object WithLocalResources {
    def unapply(runData: RunData): Option[LocalResources] = runData.resourcesOpt match {
      case Some(lrs: LocalResources) => Some(lrs)
      case _ => None
    }
  }
  
  private[jobs] def determineJobStatus(status: JobStatus): JobStatus = status match {
    case JobStatus.WaitingForOutputs => JobStatus.Succeeded
    case _ => status
  }
}
