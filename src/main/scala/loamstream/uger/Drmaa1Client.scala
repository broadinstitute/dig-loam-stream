package loamstream.uger

import java.nio.file.Path

import loamstream.conf.UgerConfig

import scala.concurrent.duration.Duration
import scala.util.Try
import org.ggf.drmaa._
import loamstream.util.Loggable
import loamstream.util.ValueBox
import loamstream.oracle.uger.Queue
import loamstream.oracle.Resources.UgerResources

/**
 * Created on: 5/19/16
 *
 * @author Kaan Yuksel 
 * @author clint
 * 
 * A DRMAAv1 implementation of DrmaaClient; can submit work to UGER and monitor it.
 */
final class Drmaa1Client(ugerClient: UgerClient) extends DrmaaClient with Loggable {
  
  import DrmaaClient._

  //NB: Several DRMAA operations are only valid if they're performed via the same Session as previous operations;
  //We use one Session per client to ensure that all operations performed by this instance use the same Session.  
  //We wrap the Session in a ValueBox to make it easier to synchronize access to it.
  private[this] lazy val sessionBox: ValueBox[Session] = ValueBox(getNewSession)
  
  private def getNewSession: Session = {
    info("Getting new DRMAA session")

    try {
      val s = SessionFactory.getFactory.getSession

      debug(s"\tVersion: ${s.getVersion}")
      debug(s"\tDrmSystem: ${s.getDrmSystem}")
      debug(s"\tDrmaaImplementation: ${s.getDrmaaImplementation}")

      //NB: Passing an empty string (or null) means that "the default DRM system is used, provided there is only one
      //DRMAA implementation available" according to the DRMAA javadocs. (Whatever that means :\)
      s.init("")

      s
    } catch {
        case e: UnsatisfiedLinkError =>
          error(s"Please check if you are running on a system with UGER (e.g. Broad VM). " +
            s"Note that UGER is required if the configuration file specifies a 'uger { ... }' block.")
          throw e
    }
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit = tryShuttingDown(sessionBox.value)
  
  /**
   * Synchronously obtain the status of one running UGER job, given its id.
   *
   * @param jobId the id of the job to get the status of
   * @return Success wrapping the JobStatus corresponding to the code obtained from UGER,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  override def statusOf(jobId: String): Try[UgerStatus] = {
    Try {
      withSession { session =>
        val status = session.getJobProgramStatus(jobId)
        val jobStatus = UgerStatus.fromUgerStatusCode(status)

        info(s"Job '$jobId' has status $status, mapped to $jobStatus")

        if (jobStatus.isFinished) {
          doWait(session, jobId, Duration.Zero)
        }
        else {
          jobStatus
        }
      }
    }
  }

  /**
   * Synchronously submit a job, in the form of a UGER wrapper shell script.
   *
   * @param ugerConfig contains the parameters to configure a job
   * @param pathToScript the path to the script that UGER should run
   * @param jobName a descriptive prefix used to identify the job.  Has no impact on how the job runs.
   * @param numTasks length of task array to be submitted as a single UGER job
   */
  override def submitJob(
                 ugerConfig: UgerConfig,
                 pathToScript: Path,
                 jobName: String,
                 numTasks: Int = 1): DrmaaClient.SubmissionResult = {

    val pathToUgerOutput = ugerConfig.logFile
    val nativeSpecification = ugerConfig.nativeSpecification
    
    runJob(pathToScript, pathToUgerOutput, nativeSpecification, jobName, numTasks)
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
  override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = {
    val waitAttempt = Try {
      withSession { session =>
        doWait(session, jobId, timeout)
      }
    }
      
    //If we time out before the job finishes, and we don't get an InvalidJobException, the job must be running 
    waitAttempt.recover {
      case e: ExitTimeoutException =>
        debug(s"Timed out waiting for job '$jobId' to finish; assuming the job's state is ${UgerStatus.Running}")
        UgerStatus.Running
      case e: InvalidJobException =>
        debug(s"Received InvalidJobException while 'wait'ing for job '$jobId'. " +
          s"Assuming that the data records of the job was already reaped by a previous call, " +
          s"and therefore mapping its status to ${UgerStatus.Done}")
        UgerStatus.Done
    }
  }
  
  private def doWait(session: Session, jobId: String, timeout: Duration): UgerStatus = {
    val jobInfo = session.wait(jobId, timeout.toSeconds)
    
    val resources = Drmaa1Client.toResources(ugerClient)(jobInfo)
      
    //Use recover for side-effect only
    resources.recover {
      case e: Exception => warn(s"Error parsing resource usage data for Job '$jobId'", e)
    }
    
    val resourcesOption = resources.toOption
    
    val result = if (jobInfo.hasExited) {
      info(s"Job '$jobId' exited with status code '${jobInfo.getExitStatus}'")
      
      val exitCode = jobInfo.getExitStatus

      UgerStatus.CommandResult(exitCode, resourcesOption)
    } else if (jobInfo.wasAborted) {
      info(s"Job '$jobId' was aborted; job info: $jobInfo")

      //TODO: Add JobStatus.Aborted?
      UgerStatus.Failed(resourcesOption)
    } else if (jobInfo.hasSignaled) {
      info(s"Job '$jobId' signaled, terminatingSignal = '${jobInfo.getTerminatingSignal}'")

      UgerStatus.Failed(resourcesOption)
    } else if (jobInfo.hasCoreDump) {
      info(s"Job '$jobId' dumped core")
      
      UgerStatus.Failed(resourcesOption)
    } else {
      info(s"Job '$jobId' finished with unknown status")

      UgerStatus.DoneUndetermined(resourcesOption)
    }
    
    debug(s"Job '$jobId' finished, returning status $result")
    
    result
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
    sessionBox.get { currentSession =>
      try { f(currentSession) } 
      catch {
        case e: DrmaaException => {
          debug(s"Got ${e.getClass.getSimpleName}; re-throwing", e)
          
          throw e
        }
      }
    }
  }
  
  private def runJob(pathToScript: Path,
                     pathToUgerOutput: Path,
                     nativeSpecification: String,
                     jobName: String,
                     numTasks: Int = 1): SubmissionResult = {

    withJobTemplate { (session, jt) =>
      val taskStartIndex = 1
      val taskEndIndex = numTasks
      val taskIndexIncr = 1

      // TODO Make native specification controllable from Loam (DSL)
      // Request 16g memory to allow Klustakwik to run in QC chunk 2. :(
      // Request short queue for faster integration testing
      // TODO: This sort of thing really, really, needs to be configurable. :(
      jt.setNativeSpecification(nativeSpecification)
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput.${JobTemplate.PARAMETRIC_INDEX}")

      import scala.collection.JavaConverters._
      
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

object Drmaa1Client {
  private[uger] def toResources(ugerClient: UgerClient)(jobInfo: JobInfo): Try[UgerResources] = {
    import scala.collection.JavaConverters._
    
    for {
      resources <- UgerResources.fromMap(jobInfo.getResourceUsage.asScala.toMap)
      jobId = jobInfo.getJobId
    } yield {
      resources.copy(
          node = ugerClient.getExecutionNode(jobId), 
          queue = ugerClient.getQueue(jobId))
    }
  }
}
