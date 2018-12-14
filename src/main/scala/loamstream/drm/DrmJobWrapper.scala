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
import loamstream.model.execute.DrmSettings
import loamstream.util.Loggable

/**
 * @author clint
 * Nov 16, 2017
 */
final case class DrmJobWrapper(
    executionConfig: ExecutionConfig,
    drmSettings: DrmSettings,
    pathBuilder: PathBuilder,
    commandLineJob: HasCommandLine,
    drmIndex: Int) extends Loggable {

  def drmStdOutPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdOutPathTemplate, drmIndex)
  }

  def drmStdErrPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdErrPathTemplate, drmIndex)
  }

  private lazy val stdOutDestPath: Path = LogFileNames.stdout(commandLineJob, executionConfig.jobOutputDir)

  private lazy val stdErrDestPath: Path = LogFileNames.stderr(commandLineJob, executionConfig.jobOutputDir)

  def outputStreams: OutputStreams = OutputStreams(stdOutDestPath, stdErrDestPath)

  private[drm] def commandLineInTaskArray: String = {
    val singularityConfig = executionConfig.singularity
    
    def mappingPart: String = {
      singularityConfig.mappedDirs.distinct.map { dir =>
        s"-B ${dir.toAbsolutePath.render} "
      }.mkString
    }

    val singularityPart = drmSettings.containerParams match {
      case Some(params) => s"${singularityConfig.executable} exec ${mappingPart}${params.imageName} "
      case _            => ""
    }

    val result = s"${singularityPart}${commandLineJob.commandLineString}"

    debug(s"Raw command in DRM shell script: '${result}'")

    result
  }

  def commandChunk(taskArray: DrmTaskArray): String = {
    val outputDir = executionConfig.jobOutputDir.toAbsolutePath

    // scalastyle:off line.size.limit
    s"""|${commandLineInTaskArray}
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |stdoutDestPath="${stdOutDestPath.render}"
        |stderrDestPath="${stdErrDestPath.render}"
        |
        |mkdir -p ${outputDir.render}
        |mv ${drmStdOutPath(taskArray).render} $$stdoutDestPath || echo "Couldn't move DRM std out log ${drmStdOutPath(
         taskArray).render}; it's likely the job wasn't submitted successfully" > $$stdoutDestPath
        |mv ${drmStdErrPath(taskArray).render} $$stderrDestPath || echo "Couldn't move DRM std err log ${drmStdErrPath(
         taskArray).render}; it's likely the job wasn't submitted successfully" > $$stderrDestPath
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}
