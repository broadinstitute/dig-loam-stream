package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.conf.UgerConfig
import loamstream.util.Maps
import loamstream.util.Traversables
import loamstream.util.Files
import java.nio.file.Path
import java.io.File
import loamstream.util.BashScript.Implicits._
import loamstream.util.Loggable
import org.ggf.drmaa.JobTemplate
import loamstream.conf.ExecutionConfig

/**
 * @author clint
 * Nov 13, 2017
 */
final case class UgerTaskArray(
    ugerConfig: UgerConfig,
    ugerJobs: Seq[UgerJobWrapper],
    ugerJobName: String,
    stdOutPathTemplate: String,
    stdErrPathTemplate: String) extends Loggable {

  def size: Int = ugerJobs.size

  lazy val scriptContents: String = ScriptBuilder.buildFrom(this)

  //NB: Side-effecting
  lazy val ugerScriptFile: Path = writeUgerScriptFile()

  private def writeUgerScriptFile(): Path = {
    val ugerWorkDir = ugerConfig.workDir

    val ugerScript = createScriptFileIn(ugerWorkDir)(scriptContents)

    trace(s"Made script '$ugerScript' from ${ugerJobs.map(_.ugerCommandChunk(this))}")

    ugerScript
  }

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFileIn(directory: Path)(contents: String): Path = {
    createScriptFile(contents, Files.tempFile(".sh", directory.toFile))
  }

  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)

    file
  }
}

object UgerTaskArray {
  /**
   * Make a name that will be used as a base for the names of all the jobs in this task array.
   *  
   * NB: This needs to be unique, and not too long, per Uger requirements.  Task arrays with job names that are too 
   * long can be submitted, but their jobs will all fail.
   *  
   */
  private[uger] def makeJobName(): String = {
    val uuid = java.util.UUID.randomUUID.toString
    
    s"LoamStream-${uuid}"
  }

  def fromCommandLineJobs(
      executionConfig: ExecutionConfig,
      ugerConfig: UgerConfig,
      jobs: Seq[CommandLineJob],
      ugerJobName: String = makeJobName()): UgerTaskArray = {

    val ugerJobs = jobs.zipWithIndex.map { case (commandLineJob, i) =>
      //Uger task array indices start from 1
      val indexInTaskArray = i + 1

      UgerJobWrapper(executionConfig, commandLineJob, indexInTaskArray)
    }

    val stdOutPathTemplate = ugerStdOutPathTemplate(ugerConfig, ugerJobName)
    val stdErrPathTemplate = ugerStdErrPathTemplate(ugerConfig, ugerJobName)

    UgerTaskArray(ugerConfig, ugerJobs, ugerJobName, stdOutPathTemplate, stdErrPathTemplate)
  }

  private[uger] def ugerStdOutPathTemplate(ugerConfig: UgerConfig, ugerJobName: String): String = {
    makeErrorOrOutputPath(ugerConfig, ugerJobName, "stdout")
  }

  private[uger] def ugerStdErrPathTemplate(ugerConfig: UgerConfig, ugerJobName: String): String = {
    makeErrorOrOutputPath(ugerConfig, ugerJobName, "stderr")
  }

  private def makeErrorOrOutputPath(ugerConfig: UgerConfig, jobName: String, suffix: String): String = {
    s":${ugerConfig.workDir.render}/$jobName.${JobTemplate.PARAMETRIC_INDEX}.$suffix"
  }
}
