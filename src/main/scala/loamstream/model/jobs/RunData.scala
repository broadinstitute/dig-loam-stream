package loamstream.model.jobs

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.Resources

/**
 * @author clint
 * Jan 3, 2018
 * 
 * Allows capturing info about the execution of a job without eagerly creating an Execution, which
 * would mean hasing any output files/uris. Deferring the creation of an Execution allows waiting to do so
 * until all outputs are present (needed to work around Uger/Broad FS issue where files created by jobs don't
 * become visible to the LS process until later.
 */
final case class RunData(
    job: LJob,
    jobStatus: JobStatus, 
    jobResult: Option[JobResult],
    resourcesOpt: Option[Resources] = None, 
    outputStreamsOpt: Option[OutputStreams] = None) {

  override def toString: String = {
    s"${getClass.getSimpleName}(Job#${job.id}, $jobStatus, $jobResult, $resourcesOpt, $outputStreamsOpt)"
  }
  
  def withResources(r: Resources): RunData = copy(resourcesOpt = Some(r))
  
  private def cmdOpt: Option[String] = job match { 
    case clj: CommandLineJob => Some(clj.commandLineString)
    case _ => None
  }
  
  //NB: This is lazy, to allow waiting to hash outputs (done by `job.outputs.map(_.toOutputRecord)`) 
  //until they're ready.
  lazy val execution: Execution = Execution(
      env = job.executionEnvironment,
      cmd = cmdOpt,
      status = jobStatus,
      result = jobResult,
      resources = resourcesOpt,
      outputs = job.outputs.map(_.toOutputRecord),
      outputStreams = outputStreamsOpt)
}
