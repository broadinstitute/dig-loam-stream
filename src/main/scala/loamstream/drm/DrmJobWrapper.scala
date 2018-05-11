package loamstream.drm

import java.nio.file.Path
import java.nio.file.Paths

import org.ggf.drmaa.JobTemplate

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.LogFileNames
import loamstream.model.jobs.OutputStreams
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.BashScript.Implicits._
import loamstream.conf.DrmConfig


/**
 * @author clint
 * Nov 16, 2017
 */
final case class DrmJobWrapper(
    executionConfig: ExecutionConfig,
    pathBuilder: PathBuilder,
    commandLineJob: HasCommandLine, 
    drmIndex: Int) {

  def drmStdOutPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdOutPathTemplate, drmIndex)
  }

  def drmStdErrPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdErrPathTemplate, drmIndex)
  }

  private lazy val stdOutDestPath: Path = LogFileNames.stdout(commandLineJob, executionConfig.jobOutputDir)

  private lazy val stdErrDestPath: Path = LogFileNames.stderr(commandLineJob, executionConfig.jobOutputDir)

  def outputStreams: OutputStreams = OutputStreams(stdOutDestPath, stdErrDestPath)

  def commandChunk(taskArray: DrmTaskArray): String = {
    val plainCommandLine = commandLineJob.commandLineString

    val outputDir = executionConfig.jobOutputDir.toAbsolutePath

    // scalastyle:off line.size.limit
    s"""|$plainCommandLine
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |stdoutDestPath="${stdOutDestPath.render}"
        |stderrDestPath="${stdErrDestPath.render}"
        |
        |mkdir -p ${outputDir.render}
        |mv ${drmStdOutPath(taskArray).render} $$stdoutDestPath || echo "Couldn't move Uger std out log" > $$stdoutDestPath
        |mv ${drmStdErrPath(taskArray).render} $$stderrDestPath || echo "Couldn't move Uger std err log" > $$stderrDestPath
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}

