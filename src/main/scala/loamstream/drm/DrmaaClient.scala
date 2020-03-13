package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Terminable


/**
 * @author clint
 * date: Jun 29, 2016
 * 
 * A trait to represent operations that can be performed on a Uger cluster.
 * For now, these are just submitting a job and checking a job's status.
 */
trait DrmaaClient extends Terminable {
  /**
   * Synchronously submit a job to UGER.
   *
   * @param ugerConfig contains the parameters to configure a job
   * @param pathToScript the path to the script that UGER should run
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  def submitJob(
      drmSettings: DrmSettings,
      drmConfig: DrmConfig,
      taskArrayScript: DrmTaskArray): DrmSubmissionResult
    
  /**
   * Synchronously inspect the status of a job with the given ID
   *
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  def statusOf(taskId: DrmTaskId): Try[DrmStatus]

  /**
   * Wait (synchronously) for a job to complete.
   *
   * @param jobId the job ID, assigned by UGER, of the job to wait for
   * @param timeout how long to wait.  If timeout elapses and the job doesn't finish, try to determine the job's
   * status using statusOf()
   */
  def waitFor(taskId: DrmTaskId, timeout: Duration): Try[DrmStatus]
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit
  
  /**
   * Kill all jobs.
   */
  def killAllJobs(): Unit
}

