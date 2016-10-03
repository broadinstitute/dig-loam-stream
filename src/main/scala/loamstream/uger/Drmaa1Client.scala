package loamstream.uger

import java.nio.file.Path

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.ExitTimeoutException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.NoActiveSessionException
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory
import loamstream.util.Loggable
import loamstream.util.TimeEnrichments.time
import loamstream.util.ValueBox

/**
 * Created on: 5/19/16
 *
 * @author Kaan Yuksel 
 * @author clint
 * 
 * A DRMAAv1 implementation of DrmaaClient; can submit work to UGER and monitor it.
 */
final class Drmaa1Client extends DrmaaClient with Loggable {
  
  import DrmaaClient._

  //NB: Several DRMAA operations are only valid if they're performed via the same Session as previous operations;
  //previously, we used one Session per client to ensure that all operations performed by this instance use the 
  //same Session.  Now allow mutating the session variable to attempt to recover in case of NoActiveSessionException 
  //hours into a run. :\
  private[this] lazy val sessionBox: ValueBox[Session] = ValueBox(getNewSession)
  
  private def getNewSession: Session = {
    info("Getting new DRMAA session")
    
    val s = SessionFactory.getFactory.getSession

    debug(s"\tVersion: ${s.getVersion}")
    debug(s"\tDrmSystem: ${s.getDrmSystem}")
    debug(s"\tDrmaaImplementation: ${s.getDrmaaImplementation}")

    //NB: Passing an empty string (or null) means that "the default DRM system is used, provided there is only one 
    //DRMAA implementation available" according to the DRMAA javadocs. (Whatever that means :\)
    s.init("")
    
    s
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def shutdown(): Unit = tryShuttingDown(sessionBox.value)
  
  /**
   * Synchronously obtain the status of one running UGER job, given its id.
   *
   * @param jobId the id of the job to get the status of
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  override def statusOf(jobId: String): Try[JobStatus] = {
    withSession { session =>
      for {
        status <- Try(session.getJobProgramStatus(jobId))
        jobStatus = JobStatus.fromUgerStatusCode(status)
      } yield {
        info(s"Job '$jobId' has status $status, mapped to $jobStatus")
        
        jobStatus
      }
    }
  }

  /**
   * Synchronously submit a job, in the form of a UGER wrapper shell script.
   *
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
    withSession { session =>
      Try {
        doWait(session, jobId, timeout)
      }.recoverWith {
        case e: ExitTimeoutException => {
          info(s"Timed out waiting for job '$jobId' to finish, checking its status")
  
          time(s"Job '$jobId': Calling Drmaa1Client.statusOf()", debug(_)) {
            statusOf(jobId)
          }
        }
        case e: DrmaaException => {
          warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
          Failure(e)
        }
      }
    }
  }
  
  private def doWait(session: Session, jobId: String, timeout: Duration): JobStatus = {
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
  }

  private def tryShuttingDown(s: Session): Unit = {
    try { s.exit() }
    catch {
      //NB: session.exit() will fail if an exception was thrown by session.init(), or if it is invoked more than
      //once.  In those cases, there's not much we can do.
      case e: DrmaaException => warn(s"Could not properly exit DRMAA Session due to ${e.getClass.getName}", e)
    }
  }
  
  private def withSession[A](f: Session => A): A = {
    val currentSession = sessionBox.value
    
    try { f(currentSession) } 
    catch {
      case e: NoActiveSessionException =>
        warn(s"Got ${e.getClass.getSimpleName}; attempting to continue with a new Session", e)

        tryShuttingDown(currentSession)

        val newSession = getNewSession

        sessionBox.mutate(_ => newSession)

        f(newSession)
    }
  }
  
  private def runJob(pathToScript: Path,
                     pathToUgerOutput: Path,
                     jobName: String,
                     numTasks: Int = 1): SubmissionResult = {

    withJobTemplate { (session, jt) =>
      val taskStartIndex = 1
      val taskEndIndex = numTasks
      val taskIndexIncr = 1

      // TODO Make native specification controllable from Loam (DSL)
      // Request 2g memory to reduce the odds of getting queued forever. 
      jt.setNativeSpecification("-clear -cwd -shell y -b n -q long -l h_vmem=8g")
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput.${JobTemplate.PARAMETRIC_INDEX}")

      val jobIds = session.runBulkJobs(jt, taskStartIndex, taskEndIndex, taskIndexIncr).asScala.map(_.toString)
    
      info(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      SubmissionSuccess(jobIds)
    }
  }

  private def withJobTemplate[A <: SubmissionResult](f: (Session, JobTemplate) => A): SubmissionResult = {
    withSession { session =>
      val jt = session.createJobTemplate
      
      try { f(session, jt) }
      catch {
        case e: DrmaaException => {
          error(s"Error: ${e.getMessage}", e)
          
          SubmissionFailure(e)
        }
      }
      finally { session.deleteJobTemplate(jt) }
    }
  }
}
