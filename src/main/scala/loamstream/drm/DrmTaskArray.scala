package loamstream.drm

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path

import loamstream.conf.DrmConfig
import loamstream.conf.ExecutionConfig
import loamstream.model.execute.DrmSettings
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.{ Files => LFiles }
import loamstream.util.Loggable


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
  lazy val drmScriptFile: Path = writeDrmScriptFiles()

  private def writeDrmScriptFiles(): Path = {
    val drmWorkDir = drmConfig.workDir
    
    LFiles.createDirsIfNecessary(drmWorkDir)

    val drmScript = createScriptFileIn(drmWorkDir)(scriptContents)

    trace(s"Made script '$drmScript' from ${drmJobs.map(_.commandChunk(this))}")

    for {
      jobDir <- drmJobs.map(_.jobDir)
    } {
      JFiles.createDirectories(jobDir)
      
      val drmScriptInJobDir = jobDir.resolve("drm-script.sh")
      
      LFiles.copyAndOverwrite(drmScript, drmScriptInJobDir)
    }
    
    drmScript
  }

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[drm] def createScriptFileIn(directory: Path)(contents: String): Path = {
    createScriptFile(contents, LFiles.tempFile(".sh", directory.toFile))
  }

  private[drm] def createScriptFile(contents: String, file: Path): Path = LFiles.writeTo(file)(contents)
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
