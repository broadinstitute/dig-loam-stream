package loamstream.drm

import loamstream.model.execute.DrmSettings
import loamstream.util.Terminable
import monix.reactive.Observable


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
  def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult]
}
