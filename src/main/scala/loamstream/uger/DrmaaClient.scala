package loamstream.uger

import java.nio.file.Path
import scala.util.Try
import scala.concurrent.duration.Duration
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
   * @param pathToScript the path to the script that UGER should run
   * @param pathToUgerOutput the path to the log file UGER should write to
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  def submitJob(
    pathToScript: Path,
    pathToUgerOutput: Path,
    jobName: String,
    numTasks: Int = 1): DrmaaClient.SubmissionResult
    
  /**
   * Synchronously inspect the status of a job with the given ID
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  def statusOf(jobId: String): Try[JobStatus]
  
  /**
   * Wait (synchronously) for a job to complete.  
   * @param jobId the job ID, assigned by UGER, of the job to wait for
   * @param timeout how long to wait.  If timeout elapses and the job doesn't finish, try to determine the job's
   * status using statusOf()
   */
  def waitFor(jobId: String, timeout: Duration): Try[JobStatus]
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit
}

object DrmaaClient {
  def drmaa1(drmaa: Drmaa1Client): DrmaaClient = new Drmaa1Client

  sealed trait SubmissionResult {
    def isFailure: Boolean
  }

  final case class SubmissionFailure(cause: Exception) extends SubmissionResult {
    override val isFailure: Boolean = true
  }

  final case class SubmissionSuccess(jobIds: Seq[String]) extends SubmissionResult {
    override val isFailure: Boolean = false
  }
}