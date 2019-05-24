package loamstream.drm

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Files
import java.nio.file.Path
import loamstream.util.BashScript.Implicits._
import loamstream.util.Loggable
import loamstream.conf.ExecutionConfig
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.model.jobs.JobOracle


/**
 * @author clint
 * Nov 13, 2017
 */
final case class DrmTaskArray(
    drmConfig: DrmConfig,
    drmJobs: Seq[DrmJobWrapper],
    drmJobName: String,
    stdOutPathTemplate: String,
    stdErrPathTemplate: String) extends Loggable {

  def size: Int = drmJobs.size

  lazy val scriptContents: String = (new ScriptBuilder(drmConfig.scriptBuilderParams)).buildFrom(this)

  //NB: Side-effecting
  lazy val drmScriptFile: Path = writeDrmScriptFile()

  private def writeDrmScriptFile(): Path = {
    val drmWorkDir = drmConfig.scriptDir

    val drmScript = createScriptFileIn(drmWorkDir)(scriptContents)

    trace(s"Made script '$drmScript' from ${drmJobs.map(_.commandChunk(this))}")

    drmScript
  }

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[drm] def createScriptFileIn(directory: Path)(contents: String): Path = {
    createScriptFile(contents, Files.tempFile(".sh", directory.toFile))
  }

  private[drm] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)

    file
  }
}

object DrmTaskArray {
  /**
   * Make a name that will be used as a base for the names of all the jobs in this task array.
   *  
   * NB: This needs to be unique, and not too long, per Uger requirements.  Task arrays with job names that are too 
   * long can be submitted, but their jobs will all fail.
   *  
   */
  private[drm] def makeJobName(): String = {
    val uuid = java.util.UUID.randomUUID.toString
    
    s"LoamStream-${uuid}"
  }

  def fromCommandLineJobs(
      executionConfig: ExecutionConfig,
      jobOracle: JobOracle,
      drmSettings: DrmSettings,
      drmConfig: DrmConfig,
      pathBuilder: PathBuilder,
      jobs: Seq[CommandLineJob],
      jobName: String = makeJobName()): DrmTaskArray = {

    val drmJobs = jobs.zipWithIndex.map { case (commandLineJob, i) =>
      //Uger task array indices start from 1
      val indexInTaskArray = i + 1

      val jobDir = jobOracle.dirFor(commandLineJob)
      
      DrmJobWrapper(executionConfig, drmSettings, pathBuilder, commandLineJob, jobDir, indexInTaskArray)
    }

    val scriptBuilderParams = drmConfig.scriptBuilderParams
    
    val stdOutPathTemplate = pathBuilder.stdOutPathTemplate(drmConfig, jobName)
    val stdErrPathTemplate = pathBuilder.stdErrPathTemplate(drmConfig, jobName)

    DrmTaskArray(drmConfig, drmJobs, jobName, stdOutPathTemplate, stdErrPathTemplate)
  }
}
