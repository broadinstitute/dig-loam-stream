package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob
import java.util.UUID
import java.nio.file.Path
import loamstream.conf.UgerConfig
import org.ggf.drmaa.JobTemplate
import loamstream.model.jobs.LogFileNames
import java.nio.file.Paths
import loamstream.conf.ExecutionConfig

/**
 * @author clint
 * Nov 16, 2017
 */
final case class UgerJobWrapper(executionConfig: ExecutionConfig, commandLineJob: CommandLineJob, ugerIndex: Int) {
  
  def ugerStdOutPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdOutPathTemplate)
  
  def ugerStdErrPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdErrPathTemplate)
  
  private def reifyPath(template: String): Path = {
    //NB: Replace task-array-index placeholder, drop initial ':'
    val pathString = template.replace(JobTemplate.PARAMETRIC_INDEX, ugerIndex.toString).dropWhile(_ == ':')
    
    Paths.get(pathString).toAbsolutePath
  }
  
  lazy val stdOutDestPath: Path = LogFileNames.stdout(commandLineJob, executionConfig.outputDir)
  
  lazy val stdErrDestPath: Path = LogFileNames.stderr(commandLineJob, executionConfig.outputDir)
  
  def ugerCommandLine(taskArray: UgerTaskArray): String = {
    val plainCommandLine = commandLineJob.commandLineString

    val outputDir = executionConfig.outputDir.toAbsolutePath
    
    // scalastyle:off line.size.limit
    s"""|$plainCommandLine
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |mkdir -p $outputDir
        |mv ${ugerStdOutPath(taskArray)} $stdOutDestPath || echo "Couldn't move Uger std out log" > $stdOutDestPath
        |mv ${ugerStdErrPath(taskArray)} $stdErrDestPath || echo "Couldn't move Uger std err log" > $stdErrDestPath
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}

