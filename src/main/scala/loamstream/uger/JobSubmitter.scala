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
import loamstream.util.Terminable

/**
 * @author clint
 * Oct 17, 2017
 * 
 * A trait representing the notion of submitting jobs to Uger.  
 */
trait JobSubmitter extends Terminable {
  /**
   * Submit a batch of jobs to be run as a Uger task array (all packaged in one script).
   * @params jobs the jobs to submit
   * @param ugerSettings the Uger settings shared by all the jobs being submitted
   */
  def submitJobs(ugerSettings: UgerSettings, taskArray: UgerTaskArray): DrmaaClient.SubmissionResult
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
        taskArray: UgerTaskArray): DrmaaClient.SubmissionResult = {

      drmaaClient.submitJob(ugerSettings, ugerConfig, taskArray)
    }
    
    override def stop(): Unit = drmaaClient.stop()
  }
}
