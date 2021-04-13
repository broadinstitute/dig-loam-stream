package loamstream.drm

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.conf.ExecutionConfig
import loamstream.util.LogFileNames
import loamstream.model.jobs.OutputStreams
import loamstream.model.jobs.commandline.HasCommandLine
import loamstream.util.BashScript.Implicits._
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Loggable
import loamstream.conf.Locations

/**
 * @author clint
 * Nov 16, 2017
 */
final case class DrmJobWrapper(
    executionConfig: ExecutionConfig,
    drmSettings: DrmSettings,
    pathBuilder: PathBuilder,
    commandLineJob: HasCommandLine,
    jobDir: Path,
    drmIndex: Int) extends Loggable {

  def drmStdOutPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdOutPathTemplate, drmIndex)
  }

  def drmStdErrPath(taskArray: DrmTaskArray): Path = {
    pathBuilder.reifyPathTemplate(taskArray.stdErrPathTemplate, drmIndex)
  }
  
  private lazy val stdOutDestPath: Path = LogFileNames.stdout(jobDir)

  private lazy val stdErrDestPath: Path = LogFileNames.stderr(jobDir)
  
  private lazy val exitCodeDestPath: Path = LogFileNames.exitCode(jobDir)

  def outputStreams: OutputStreams = OutputStreams(stdOutDestPath, stdErrDestPath)

  private[drm] def commandLineInTaskArray: String = {
    val singularityConfig = executionConfig.singularity
    
    def mappingPart: String = {
      singularityConfig.mappedDirs.distinct.map { dir =>
        s"-B ${dir.toAbsolutePath.render} "
      }.mkString
    }

    def munge(extraParams: String): String = if(extraParams.nonEmpty) s"${extraParams} " else extraParams
    
    val singularityPart = drmSettings.containerParams match {
      case Some(ContainerParams(imageName, extraParams)) => {
        s"${singularityConfig.executable} exec ${mappingPart}${munge(extraParams)}${imageName} "
      }
      case _ => ""
    }

    val result = s"${singularityPart}${commandLineJob.commandLineString}"

    trace(s"Raw command in DRM shell script: '${result}'")

    result
  }

  def commandChunk(taskArray: DrmTaskArray): String = {
    val outputDir = jobDir.toAbsolutePath

    // scalastyle:off line.size.limit
    s"""|${commandLineInTaskArray}
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |origStdoutPath="${drmStdOutPath(taskArray).render}"
        |origStderrPath="${drmStdErrPath(taskArray).render}"
        |
        |stdoutDestPath="${stdOutDestPath.render}"
        |stderrDestPath="${stdErrDestPath.render}"
        |exitcodeDestPath="${stdErrDestPath.render}"
        |
        |jobDir="${outputDir.render}"
        |
        |mkdir -p $$jobDir
        |mv $$origStdoutPath $$stdoutDestPath || echo "Couldn't move DRM std out log $$origStdoutPath; it's likely the job wasn't submitted successfully" > $$stdoutDestPath
        |mv $$origStderrPath $$stderrDestPath || echo "Couldn't move DRM std err log $$origStderrPath; it's likely the job wasn't submitted successfully" > $$stderrDestPath
        |echo $$LOAMSTREAM_JOB_EXIT_CODE > $$exitcodeDestPath
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}
