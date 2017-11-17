package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob
import java.util.UUID
import java.nio.file.Path
import loamstream.conf.UgerConfig
import org.ggf.drmaa.JobTemplate
import loamstream.model.jobs.LogFileNames
import java.nio.file.Paths

/**
 * @author clint
 * Nov 16, 2017
 */
final case class UgerJobWrapper(commandLineJob: CommandLineJob, ugerIndex: Int) {
  
  def ugerStdOutPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdOutPathTemplate)
  
  def ugerStdErrPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdErrPathTemplate)
  
  private def reifyPath(template: String): Path = {
    //NB: Replace task-array-index placeholder, drop initial ':'
    val pathString = template.replace(JobTemplate.PARAMETRIC_INDEX, ugerIndex.toString).dropWhile(_ == ':')
    
    Paths.get(pathString).toAbsolutePath
  }
  
  def stdOutDestPath: Path = LogFileNames.stdout(commandLineJob)
  
  def stdErrDestPath: Path = LogFileNames.stderr(commandLineJob)
  
  def ugerCommandLine(taskArray: UgerTaskArray): String = {
    val plainCommandLine = commandLineJob.commandLineString

    // scalastyle:off line.size.limit
    s"( $plainCommandLine ) ; mv ${ugerStdOutPath(taskArray)} $stdOutDestPath ; mv ${ugerStdErrPath(taskArray)} $stdErrDestPath"
    // scalastyle:on line.size.limit
  }
}

