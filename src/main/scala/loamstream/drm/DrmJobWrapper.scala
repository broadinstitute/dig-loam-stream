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
import loamstream.util.CanBeClosed

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

  private[drm] lazy val commandScript: Path = {
    val scriptPath = jobDir.resolve("command.sh").toAbsolutePath

    loamstream.util.Files.createDirsIfNecessary(jobDir)

    loamstream.util.Files.writeTo(scriptPath)(commandLineInTaskArray)

    scriptPath
  }

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

    import DrmJobWrapper.timestampCommand

    val timePrefixPart = DrmJobWrapper.timePrefix()

    def drmWorkSubDir: Path = (taskArray.drmConfig.drmSystem match {
      case DrmSystem.Uger => executionConfig.locations.ugerDir
      case DrmSystem.Lsf => executionConfig.locations.lsfDir
      case DrmSystem.Slurm => executionConfig.locations.slurmDir
    }).getFileName

    // scalastyle:off line.size.limit
    s"""|jobDir="${outputDir.render}"
        |mkdir -p "$$jobDir"
        |
        |STATS_FILE="${statsFileDestPath.render}"
        |EXITCODE_FILE="${exitCodeDestPath.render}"
        |COMMAND_SCRIPT="${commandScript}"
        |STDOUT_FILE="${stdOutDestPath.render}"
        |STDERR_FILE="${stdErrDestPath.render}"
        |
        |echo "Node: $$(hostname)" >> $$STATS_FILE
        |echo Task_Array_Name: ${taskArray.drmJobName} >> $$STATS_FILE
        |echo DRM_Task_Id: "$${jobId}-$${i}" >> $$STATS_FILE
        |echo "Raw_Logs: .loamstream/${drmWorkSubDir}/${taskArray.drmJobName}/$${i}.{stdout,stderr}" >> $$STATS_FILE
        |
        |START="$$(${timestampCommand})"
        |
        |${timePrefixPart} bash $$COMMAND_SCRIPT 1> $$STDOUT_FILE 2> $$STDERR_FILE
        |
        |LOAMSTREAM_JOB_EXIT_CODE=$$?
        |
        |echo "$$LOAMSTREAM_JOB_EXIT_CODE" > $$EXITCODE_FILE
        |
        |echo "Start: $$START" >> $$STATS_FILE
        |echo "End: $$(${timestampCommand})" >> $$STATS_FILE
        |
        |exit $$LOAMSTREAM_JOB_EXIT_CODE
        |""".stripMargin
    // scalastyle:on line.size.limit
  }
}

object DrmJobWrapper {
  private[drm] def timePrefix(statsFileEnvVarName: String = "STATS_FILE"): String = {
    //NB: Memory will be max-rss, in kilobytes.
    //System is time spent in kernel code, user is time spent in user code.

    //val formatSpec = "ExitCode: %x\\nMemory: %Mk\\nSystem: %Ss\\nUser: %Us"

    //Use `which time` to find the 'time' binary on the path (we do NOT want the Bash builtin 'time').
    //-o makes output specified by $formatSpec go to $statsFile.

    //s"""`which time` -o $$${statsFileEnvVarName} --format="${formatSpec}""""
    s"""time """

  }

  private[drm] val timestampCommand: String = """date +%Y-%m-%dT%H:%M:%S"""
}