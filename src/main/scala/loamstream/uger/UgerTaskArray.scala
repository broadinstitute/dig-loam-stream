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
   * A Uger job name, like `LoamStream-<uger-job-id-0>_<uger-job-id-1>..._<uger-job-id-N>`
   * NB: Built deterministically.
   */

  private var jobIdMap = Map[String,Seq[Int]]()

  // lookup a list of job IDs from the job SHA
  def jobIdsOfSha(sha: String): Option[Seq[Int]] = jobIdMap.get(sha)

  // hash a list of job IDs, which can grow too long for Uger
  private[uger] def hashJobIds(jobIds: Seq[Int]): String =
    java.security.MessageDigest.getInstance("SHA-1")
      .digest(jobIds.mkString("_").getBytes)
      .map((b: Byte) => (if (b >= 0 & b < 16) "0" else "") + (b & 0xFF).toHexString)
      .mkString

  // the job name is the hash of all the job IDs
  private[uger] def makeJobName(jobs: Seq[CommandLineJob]): String = {
    val jobIds = jobs.map(_.id)
    val sha = hashJobIds(jobIds)

    // record this short -> long name in the map (thread safe)
    synchronized {
      jobIdMap += (sha -> jobIds)
    }

    s"LoamStream-${sha}"
  }

  def fromCommandLineJobs(
      executionConfig: ExecutionConfig,
      ugerConfig: UgerConfig,
      jobs: Seq[CommandLineJob]): UgerTaskArray = {

    val ugerJobName: String = makeJobName(jobs)

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
