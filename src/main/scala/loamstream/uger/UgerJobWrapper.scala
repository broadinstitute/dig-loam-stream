package loamstream.uger

import java.nio.file.Path
import java.nio.file.Paths

import org.ggf.drmaa.JobTemplate

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.LogFileNames
import loamstream.model.jobs.OutputStreams
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.BashScript.Implicits._

/**
 * @author clint
 * Nov 16, 2017
 */
final case class UgerJobWrapper(executionConfig: ExecutionConfig, commandLineJob: HasCommandLine, ugerIndex: Int) {

  def ugerStdOutPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdOutPathTemplate)

  def ugerStdErrPath(taskArray: UgerTaskArray): Path = reifyPath(taskArray.stdErrPathTemplate)

  private def reifyPath(template: String): Path = {
    //NB: Replace task-array-index placeholder, drop initial ':'
    val pathString = template.replace(JobTemplate.PARAMETRIC_INDEX, ugerIndex.toString).dropWhile(_ == ':')

    Paths.get(pathString).toAbsolutePath
  }

  private lazy val stdOutDestPath: Path = LogFileNames.stdout(commandLineJob, executionConfig.jobOutputDir)

  private lazy val stdErrDestPath: Path = LogFileNames.stderr(commandLineJob, executionConfig.jobOutputDir)

  def outputStreams: OutputStreams = OutputStreams(stdOutDestPath, stdErrDestPath)

  def ugerCommandChunk(taskArray: UgerTaskArray): String = {
    val plainCommandLine = commandLineJob.commandLineString

    val outputDir = executionConfig.jobOutputDir.toAbsolutePath

    // scalastyle:off line.size.limit
    s"""|$plainCommandLine
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |mkdir -p ${outputDir.render}
        |mv ${ugerStdOutPath(taskArray).render} ${stdOutDestPath.render} || echo "Couldn't move Uger std out log" > ${stdOutDestPath.render}
        |mv ${ugerStdErrPath(taskArray).render} ${stdErrDestPath.render} || echo "Couldn't move Uger std err log" > ${stdErrDestPath.render}
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}

