package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.conf.UgerConfig
import loamstream.util.Loggable
import loamstream.model.execute.UgerSettings
import java.nio.file.Path
import loamstream.util.Files
import java.io.File
import java.time.format.DateTimeFormatter
import java.time.ZoneId
import java.util.UUID
import java.time.Instant

/**
 * @author clint
 * Oct 17, 2017
 * 
 * A trait representing the notion of submitting jobs to Uger.  
 */
trait JobSubmitter {
  /**
   * Submit a batch of jobs to be run as a Uger task array (all packaged in one script).
   * @params jobs the jobs to submit
   * @param ugerSettings the Uger settings shared by all the jobs being submitted
   */
  def submitJobs(ugerSettings: UgerSettings, jobs: Seq[CommandLineJob]): DrmaaClient.SubmissionResult
}

object JobSubmitter {
  /**
   * @author clint
   * Oct 17, 2017
   * 
   * Default implementation of JobSubmitter; uses a DrmaaClient to submit jobs. 
   */
  final case class Drmaa(drmaaClient: DrmaaClient, ugerConfig: UgerConfig) extends JobSubmitter with Loggable {
    override def submitJobs(
        ugerSettings: UgerSettings,
        ugerJobs: Seq[CommandLineJob]): DrmaaClient.SubmissionResult = {
      
      val ugerScript = writeUgerScriptFile(ugerJobs)

      drmaaClient.submitJob(ugerSettings, ugerConfig, ugerScript, makeJobName(), ugerJobs.size)
    }
    
    private def writeUgerScriptFile(commandLineJobs: Seq[CommandLineJob]): Path = {
      val ugerWorkDir = ugerConfig.workDir.toFile
    
      val ugerScript = createScriptFile(ScriptBuilder.buildFrom(commandLineJobs), ugerWorkDir)
    
      trace(s"Made script '$ugerScript' from $commandLineJobs")
    
      ugerScript
    }
  }
    
  private[uger] def createScriptFile(contents: String, file: Path): Path = {
    Files.writeTo(file)(contents)

    file
  }

  /**
   * Creates a script file in the *default temporary-file directory*, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String): Path = createScriptFile(contents, Files.tempFile(".sh"))

  /**
   * Creates a script file in the *specified* directory, using
   * the given prefix and suffix to generate its name.
   */
  private[uger] def createScriptFile(contents: String, directory: File): Path = {
    createScriptFile(contents, Files.tempFile(".sh", directory))
  }
  
  private val formatter: DateTimeFormatter = {
    DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss").withZone(ZoneId.systemDefault)
  }

  /**
   * Makes a Uger job name, like `LoamStream-<date>-<UUID>`
   */
  //TODO: Test
  private[uger] def makeJobName(timestamp: Instant = Instant.now, uuid: UUID = UUID.randomUUID): String = {
    s"LoamStream-${formatter.format(timestamp)}-$uuid"
  }
}
