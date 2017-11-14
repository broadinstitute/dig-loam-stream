package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob

/**
 * @author clint
 * Nov 13, 2017
 */
final class UgerTaskArrayScript(commandLineJobs: Seq[CommandLineJob]) {
  lazy val scriptContents: String = ScriptBuilder.buildFrom(commandLineJobs)
  
  lazy val jobsToUgerIndices: Map[CommandLineJob, Int] = ScriptBuilder.jobsWithUgerIndices(commandLineJobs).toMap
  
  def ugerIndexOf(commandLineJob: CommandLineJob): Option[Int] = jobsToUgerIndices.get(commandLineJob)
  
  /*def jobNamesFor(commandLineJob: CommandLineJob): Seq[String] = {
    ugerIndexOf(commandLineJob).flatMap { taskArrayIndex =>
      s""
    }
  }*/
}
