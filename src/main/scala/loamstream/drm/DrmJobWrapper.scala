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

  private def makePath(template: String): Path = pathBuilder.reifyPathTemplate(template, drmIndex)
  
  def drmStdOutPath(taskArray: DrmTaskArray): Path = makePath(taskArray.stdOutPathTemplate)

  def drmStdErrPath(taskArray: DrmTaskArray): Path = makePath(taskArray.stdErrPathTemplate)
  
  private lazy val stdOutDestPath: Path = LogFileNames.stdout(jobDir)

  private lazy val stdErrDestPath: Path = LogFileNames.stderr(jobDir)
  
  private lazy val exitCodeDestPath: Path = LogFileNames.exitCode(jobDir)

  private[drm] lazy val statsFileDestPath: Path = LogFileNames.stats(jobDir)

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

    val timePrefixPart = DrmJobWrapper.timePrefix(statsFileDestPath)

    val result = s"${timePrefixPart} ${singularityPart}${commandLineJob.commandLineString}"

    trace(s"Raw command in DRM shell script: '${result}'")

    result
  }

  def commandChunk(taskArray: DrmTaskArray): String = {
    val outputDir = jobDir.toAbsolutePath

    import DrmJobWrapper.timestampCommand

    // scalastyle:off line.size.limit
    s"""|jobDir="${outputDir.render}"
        |mkdir -p $$jobDir
        |
        |START="$$(${timestampCommand})"
        |
        |${commandLineInTaskArray}
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |echo "Start: $$START\\nEnd: $$(${timestampCommand})" >> ${statsFileDestPath}
        |
        |origStdoutPath="${drmStdOutPath(taskArray).render}"
        |origStderrPath="${drmStdErrPath(taskArray).render}"
        |
        |stdoutDestPath="${stdOutDestPath.render}"
        |stderrDestPath="${stdErrDestPath.render}"
        |exitcodeDestPath="${exitCodeDestPath.render}"
        |
        |echo $$LOAMSTREAM_JOB_EXIT_CODE > $$exitcodeDestPath
        |
        |mv $$origStdoutPath $$stdoutDestPath || echo "Couldn't move DRM std out log $$origStdoutPath; it's likely the job wasn't submitted successfully" > $$stdoutDestPath
        |mv $$origStderrPath $$stderrDestPath || echo "Couldn't move DRM std err log $$origStderrPath; it's likely the job wasn't submitted successfully" > $$stderrDestPath
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}

object DrmJobWrapper {
  private[drm] def timePrefix(statsFile: Path): String = {
    //NB: Memory will be max-rss, in kilobytes.
    //System is time spent in kernel code, user is time spent in user code.
    val formatSpec = "ExitCode: %x\\nMemory: %Mk\\nSystem: %Ss\\nUser: %Us"

    //Use `which time` to find the 'time' binary on the path (we do NOT want the Bash builtin 'time').
    //-o makes output specified by $formatSpec go to $statsFile.
    s"""`which time` -o ${statsFile} --format="${formatSpec}""""
  }

  private[drm] val timestampCommand: String = """date +%Y-%m-%dT%H:%M:%S"""
}