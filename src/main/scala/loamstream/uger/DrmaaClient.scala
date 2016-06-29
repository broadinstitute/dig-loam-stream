package loamstream.uger

import java.nio.file.Path
import scala.util.Try
import loamstream.util.Loggable
import org.ggf.drmaa.Session
import loamstream.uger.JobStatus.Undetermined

/**
 * @author clint
 * date: Jun 29, 2016
 * 
 * A trait to represent operations that can be performed on a Uger cluster.
 * For now, these are just submitting a job and checking a job's status.
 */
trait DrmaaClient {
  /**
   * Submit a job to UGER.  
   * @param pathToScript the path to the script that UGER should run
   * @param pathToUgerOutput the path to the log file UGER should write to
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   */
  def submitJob(
    pathToScript: Path,
    pathToUgerOutput: Path,
    jobName: String): DrmaaClient.SubmissionResult

  /**
   * Inspect the status of a job with the given ID
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  def statusOf(jobId: String): Try[JobStatus]
}

object DrmaaClient {
  def drmaa1(drmaa: Drmaa1Client): DrmaaClient = new Drmaa1Client

  sealed trait SubmissionResult {
    def isFailure: Boolean
  }

  final case class Failure(cause: Exception) extends SubmissionResult {
    override val isFailure: Boolean = true
  }

  final case class SingleJobSubmissionResult(jobId: String) extends SubmissionResult {
    override val isFailure: Boolean = false
  }

  final case class BulkJobSubmissionResult(jobIds: Seq[Any]) extends SubmissionResult {
    override val isFailure: Boolean = false
  }
}