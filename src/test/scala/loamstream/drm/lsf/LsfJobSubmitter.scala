package loamstream.drm.lsf

import loamstream.drm.JobSubmitter
import loamstream.model.execute.DrmSettings
import loamstream.drm.DrmTaskArray
import loamstream.drm.DrmSubmissionResult

/**
 * @author clint
 * May 11, 2018
 */
final class LsfJobSubmitter extends JobSubmitter {
  override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): DrmSubmissionResult = {

    //TODO
    
    ???
  }
    
  override def stop(): Unit = ??? //TODO
}
