package loamstream.uger

import java.nio.file.Path

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.Try

import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.ExitTimeoutException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory

import loamstream.util.Loggable

/**
 * Created on: 5/19/16 
 * @author Kaan Yuksel 
 * @author clint
 * 
 * A DRMAAv1 implementation of DrmaaClient; can submit work to UGER and monitor it.
 */
final class Drmaa1Client extends DrmaaClient with Loggable {
  
  import DrmaaClient._

  // Maximum number of tasks to be bundled as an array and submitted as a single job
  // The way the UGER scripts are generated, 2500 is about the limit.
  val MAX_NUM_TASKS = 2400

  //NB: Several DRMAA operations are only valid if they're performed via the same Session as previous operations;
  //use one Session per client to ensure that all operations performed by this instance use the same Session.
  private[this] lazy val session: Session = {
    val s = SessionFactory.getFactory.getSession
    
    s.init("")
    
    s
  }

  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def shutdown(): Unit = {
    try { session.exit() }
    catch {
      //NB: session.exit() will fail if an exception was thrown by session.init(), or if it is invoked more than
      //once.  In those cases, there's not much we can do.
      case e: DrmaaException => ()
    }
  }
  
  /**
   * Synchronously obtain the status of one running UGER job, given its id.
   * @param jobId the id of the job to get the status of
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  override def statusOf(jobId: String): Try[JobStatus] = {
    for {
      status <- Try(session.getJobProgramStatus(jobId))
      jobStatus = JobStatus.fromUgerStatusCode(status)
    } yield {
      info(s"Job '$jobId' has status $status, mapped to $jobStatus")
      
      jobStatus
    }
  }

  /**
   * Synchronously submit a job, in the form of a UGER wrapper shell script.
   * @param pathToScript the wrapper script to submit
   * @param pathToUgerOutput a path pointing to the desired location of log output from UGER
   * @param jobName a descriptive, human-readable name for the submitted work
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  override def submitJob(
      pathToScript: Path,
      pathToUgerOutput: Path,
      jobName: String,
      numTasks: Int = 1): DrmaaClient.SubmissionResult = {

    require(1 to MAX_NUM_TASKS contains numTasks)
    runJob(pathToScript, pathToUgerOutput, jobName, numTasks)
  }
  
  /**
   * Synchronously wait for the timeout period for the job with the given id to complete.
   * If the timeout period elapses and the job doesn't complete, assume the job is still running, and inquire
   * about its status with statusOf.
   * 
   * @param jobId the job id to wait for
   * @param timeout how long to wait (note that this method can be called many times)
   * @return Success with a JobStatus reflecting the completion status of the job, or the result of statusOf()
   * if the timeout elapses without the job finishing.  Otherwise, return a Failure if the job's status can't be 
   * determined. 
   * 
   * Specifically: 
   *   JobStatus.Done: Job finishes with status code 0
   *   JobStatus.Failed: Job exited with a non-zero return code, OR the job was aborted, OR the job ended due to a 
   *   signal, OR the job dumped core/
   *   JobStatus.Undetermined: The job completed, but none of the above applies.
   */
  override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = {
    Try {
      val jobInfo = session.wait(jobId, timeout.toSeconds)
      
      if (jobInfo.hasExited) {
        info(s"Job '$jobId' exited with status code '${jobInfo.getExitStatus}'")
        
        //TODO: More flexibility?
        jobInfo.getExitStatus match { 
          case 0 => JobStatus.Done
          case _ => JobStatus.Failed
        }
      } else if (jobInfo.wasAborted) {
        info(s"Job '$jobId' was aborted; job info: $jobInfo")

        //TODO: Add JobStatus.Aborted?
        JobStatus.Failed
      } else if (jobInfo.hasSignaled) {
        info(s"Job '$jobId' signaled, terminatingSignal = '${jobInfo.getTerminatingSignal}'")

        JobStatus.Failed
      } else if (jobInfo.hasCoreDump) {
        info(s"Job '$jobId' dumped core")
        
        JobStatus.Failed
      } else {
        info(s"Job '$jobId' finished with unknown status")
        
        JobStatus.DoneUndetermined
      }
    }.recoverWith {
      case e: ExitTimeoutException => {
        info(s"Timed out waiting for job '$jobId' to finish, checking its status")
        
        statusOf(jobId)
      }
    }
  }
  
  private def runJob(pathToScript: Path,
                     pathToUgerOutput: Path,
                     jobName: String,
                     numTasks: Int = 1): SubmissionResult = {

    require(1 to MAX_NUM_TASKS contains numTasks)

    withJobTemplate(session) { jt =>
      val taskStartIndex = 1
      val taskEndIndex = numTasks
      val taskIndexIncr = 1

      // TODO Make native specification controllable from Loam (DSL)
      jt.setNativeSpecification("-clear -cwd -shell y -b n -q long -l m_mem_free=16g")
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput.${JobTemplate.PARAMETRIC_INDEX}")

      val jobIds = session.runBulkJobs(jt, taskStartIndex, taskEndIndex, taskIndexIncr).asScala.map(_.toString)
    
      info(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      SubmissionSuccess(jobIds)
    }
  }

  private def withJobTemplate[A <: SubmissionResult](session: Session)(f: JobTemplate => A): SubmissionResult = {
    val jt = session.createJobTemplate
    
    try { f(jt) }
    catch {
      case e: DrmaaException => {
        error(s"Error: ${e.getMessage}", e)
        
        SubmissionFailure(e)
      }
    }
    finally { session.deleteJobTemplate(jt) }
  }
}
