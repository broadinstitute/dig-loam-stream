package loamstream.drm

import java.nio.file.Path
import scala.concurrent.duration.Duration
import scala.util.Try
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.ExitTimeoutException
import org.ggf.drmaa.InvalidJobException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Classes.simpleNameOf
import loamstream.util.CompositeException
import loamstream.util.Loggable
import loamstream.util.OneTimeLatch
import loamstream.util.Throwables
import loamstream.util.ValueBox
import org.ggf.drmaa.JobInfo


/**
 * Created on: 5/19/16
 *
 * @author Kaan Yuksel
 * @author clint
 *
 * A DRMAAv1 implementation of DrmaaClient; can submit work to UGER and monitor it.
 *
 */
final class Drmaa1Client(nativeSpecBuilder: NativeSpecBuilder) extends DrmaaClient with Loggable {
  
  /*
   * NOTE: BEWARE: DRMAAv1 is not thread-safe.  All operations on org.ggf.drmaa.Sessions that change the number
   * of remote jobs - either by submitting them, killing them, or otherwise altering them with Session.control() -
   * need to be synchronized; they can't happen concurrently.
   * 
   * Currently, this is handled by doing all operations that need a Session inside a call to withSession().
   */

  /*
   * NB: Several DRMAA operations are only valid if they're performed via the same Session as previous operations;
   * We use one Session per client to ensure that all operations performed by this instance use the same Session.
   * We wrap the Session in a ValueBox to make it easier to synchronize access to it.   
   */
  private[this] lazy val sessionBox: ValueBox[Session] = ValueBox(getNewSession)

  //Latch to ensure we only stop() once
  private[this] val stopLatch: OneTimeLatch = new OneTimeLatch

  private def getNewSession: Session = {
    debug("Getting new DRMAA session")

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
   * Kill the job with the specified id, if the job is running.
   */
  override def killJob(jobId: String): Unit = {
    debug(s"Killing Job '$jobId'")

    withSession(_.control(jobId, Session.TERMINATE))
  }

  /**
   * Kill all jobs.
   */
  override def killAllJobs(): Unit = {
    debug(s"Killing all jobs...")

    killJob(Session.JOB_IDS_SESSION_ALL)
  }

  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc).
   * Only the first call will do anything; subsequent calls won't have any effect.
   */
  override def stop(): Iterable[Throwable] = Throwables.failureOption {
    stopLatch.doOnce {
      withSession { session =>
        val failures = Throwables.collectFailures(
          () => killAllJobs(),
          () => tryShuttingDown(session))
  
        if (failures.nonEmpty) {
          throw new CompositeException(failures)
        }
      }
    }
  }

  /**
   * Synchronously obtain the status of one running DRM job, given its id.
   *
   * @param jobId the id of the job to get the status of
   * @return Success wrapping the JobStatus corresponding to the code obtained from DRM,
   * or Failure if the job id isn't known.  (Lamely, this can occur if the job is finished.)
   */
  override def statusOf(jobId: String): Try[DrmStatus] = {
    Try {
      withSession { session =>
        val status = session.getJobProgramStatus(jobId)
        val jobStatus = DrmStatus.fromDrmStatusCode(status)

        debug(s"Job '$jobId' has status $status, mapped to $jobStatus")

        if (jobStatus.isFinished) {
          doWait(session, jobId, Duration.Zero)
        } else {
          jobStatus
        }
      }
    }
  }

  /**
   * Synchronously submit a job, in the form of a DRM wrapper shell script.
   *
   * @param drmSettings job-specific settings (number of cores, memory, etc requested)
   * @param drmConfig contains execution-wide parameters
   * @param taskArray 
   */
  override def submitJob(
    drmSettings: DrmSettings,
    drmConfig: DrmConfig,
    taskArray: DrmTaskArray): DrmSubmissionResult = {

    val fullNativeSpec = nativeSpecBuilder.toNativeSpec(drmSettings)

    runJob(taskArray, drmConfig.workDir, fullNativeSpec)
  }

  /**
   * Synchronously wait for the timeout period for the job with the given id to complete.
   * If the timeout period elapses and the job doesn't complete, assume the job is still running, and inquire
   * about its status with statusOf.
   *
   * @param jobId the job id to wait for
   * @param timeout how long to wait (note that this method can be called many times)
   * @return Success with a JobStatus reflecting the completion status of the job, or a Failure.
   * If the timeout elapses without the job finishing, return Success(Running).  If an InvalidJobException
   * is thrown while waiting, return Success(Done).
   */
  override def waitFor(jobId: String, timeout: Duration): Try[DrmStatus] = {
    val waitAttempt = Try {
      withSession { session =>
        doWait(session, jobId, timeout)
      }
    }

    //If we time out before the job finishes, and we don't get an InvalidJobException, the job must be running 
    waitAttempt.recover {
      case e: ExitTimeoutException =>
        debug(s"Timed out waiting for job '$jobId' to finish; assuming the job's state is ${DrmStatus.Running}")
        DrmStatus.Running
      case e: InvalidJobException =>
        debug(s"Received InvalidJobException while 'wait'ing for job '$jobId'. " +
          s"Assuming that the data records of the job was already reaped by a previous call, " +
          s"and therefore mapping its status to ${DrmStatus.Done}")
        DrmStatus.Done
    }
  }

  private def doWait(session: Session, jobId: String, timeout: Duration): DrmStatus = {
    val jobInfo = session.wait(jobId, timeout.toSeconds)

    val result = if (jobInfo.hasExited) {
      val exitCode = jobInfo.getExitStatus

      debug(s"Job '$jobId' exited with status code '${exitCode}'")

      DrmStatus.CommandResult(exitCode)
    } else if (jobInfo.wasAborted) {
      info(s"Job '$jobId' was aborted")

      //TODO: Add JobStatus.Aborted?
      DrmStatus.Failed
    } else if (jobInfo.hasSignaled) {
      info(s"Job '$jobId' signaled, terminatingSignal = '${jobInfo.getTerminatingSignal}'")

      DrmStatus.Failed
    } else if (jobInfo.hasCoreDump) {
      info(s"Job '$jobId' dumped core")

      DrmStatus.Failed
    } else {
      debug(s"Job '$jobId' finished with unknown status")

      DrmStatus.DoneUndetermined
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
          debug(s"Got ${simpleNameOf(e)}; re-throwing", e)

          throw e
        }
      }
    }
  }

  private def runJob(taskArray: DrmTaskArray,
                     outputDir: Path,
                     nativeSpecification: String): DrmSubmissionResult = {

    withJobTemplate { (session, jt) =>
      val taskStartIndex = 1
      val taskEndIndex = taskArray.size
      val taskIndexIncr = 1

      val jobName = taskArray.drmJobName
      val pathToScript = taskArray.drmScriptFile

      debug(s"Using native spec: '$nativeSpecification'")
      debug(s"Using job name: '$jobName'")
      debug(s"Using script: '$pathToScript'")

      jt.setNativeSpecification(nativeSpecification)
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      //NB: Where a job's stdout goes
      jt.setOutputPath(taskArray.stdOutPathTemplate)
      //NB: Where a job's stderr goes
      jt.setErrorPath(taskArray.stdErrPathTemplate)

      import scala.collection.JavaConverters._

      val jobIds = session.runBulkJobs(jt, taskStartIndex, taskEndIndex, taskIndexIncr).asScala.map(_.toString)

      debug(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      val idsForJobs = jobIds.zip(taskArray.drmJobs).toMap

      def drmIdsToJobsString = {
        (for {
          (drmId, job) <- idsForJobs.mapValues(_.commandLineJob)
        } yield {
          s"DRM Id: $drmId => $job"
        }).mkString("\n")
      }

      info(s"DRM ids assigned to jobs:\n$drmIdsToJobsString")

      DrmSubmissionResult.SubmissionSuccess(idsForJobs)
    }
  }

  private def withJobTemplate[A <: DrmSubmissionResult](f: (Session, JobTemplate) => A): DrmSubmissionResult = {
    withSession { session =>
      val jt = session.createJobTemplate

      try { f(session, jt) }
      catch {
        case e: DrmaaException => {
          error(s"Error: ${e.getMessage}", e)

          DrmSubmissionResult.SubmissionFailure(e)
        }
      } finally { session.deleteJobTemplate(jt) }
    }
  }
}
