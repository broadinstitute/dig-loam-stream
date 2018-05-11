package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Loggable
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
  def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult
}

object JobSubmitter {
  /**
   * @author clint
   * Oct 17, 2017
   * 
   * Default implementation of JobSubmitter; uses a DrmaaClient to submit jobs. 
   */
  final case class Drmaa(drmaaClient: DrmaaClient, drmConfig: DrmConfig) extends JobSubmitter with Loggable {
    override def submitJobs(
        drmSettings: DrmSettings,
        taskArray: DrmTaskArray): DrmSubmissionResult = {

      drmaaClient.submitJob(drmSettings, drmConfig, taskArray)
    }
    
    override def stop(): Unit = drmaaClient.stop()
  }
}
