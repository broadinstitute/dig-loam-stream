package loamstream.uger

import loamstream.util.Loggable
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory
import scala.collection.JavaConverters._
import java.nio.file.Path
import org.ggf.drmaa.NoActiveSessionException
import scala.util.Try
import scala.concurrent.duration.Duration
import org.ggf.drmaa.ExitTimeoutException
import scala.util.control.NonFatal

/**
 * Created on: 5/19/16 
 * @author Kaan Yuksel 
 * @author clint
 */
final class Drmaa1Client extends DrmaaClient with Loggable {
  
  import DrmaaClient._
  
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
      //once.  In that case, there's not much we can do.
      case e: NoActiveSessionException => ()
    }
  }
  
  override def statusOf(jobId: String): Try[JobStatus] = {
    for {
      status <- Try(session.getJobProgramStatus(jobId))
      jobStatus = JobStatus.fromUgerStatusCode(status)
    } yield {
      println(s"statusOf(): Job '$jobId' has status $status, mapped to $jobStatus")
      
      jobStatus
    }
  }
  
  override def submitJob(
      pathToScript: Path,
      pathToUgerOutput: Path,
      jobName: String): DrmaaClient.SubmissionResult = {

    runJob(pathToScript, pathToUgerOutput, true, jobName)
  }
  
  override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = {
    Try {
      val jobInfo = session.wait(jobId, timeout.toSeconds.toLong)
      
      if (jobInfo.hasExited) {
        println(s"Job '$jobId' exited with status code '${jobInfo.getExitStatus}'")
        
        //TODO: More flexibility?
        jobInfo.getExitStatus match { 
          case 0 => JobStatus.Done
          case _ => JobStatus.Failed
        }
      } else if (jobInfo.wasAborted) {
        println(s"Job '$jobId' was aborted; job info: $jobInfo")

        //TODO: Add JobStatus.Aborted?
        JobStatus.Failed
      } else if (jobInfo.hasSignaled) {
        println(s"Job '$jobId' signaled, terminatingSignal = '${jobInfo.getTerminatingSignal}'")

        JobStatus.Failed
      } else if (jobInfo.hasCoreDump) {
        println(s"Job '$jobId' dumped core")
        
        JobStatus.Failed
      } else {
        println(s"Job '$jobId' finished with unknown status")
        
        JobStatus.Done
      }
    }.recoverWith {
      case e: ExitTimeoutException => {
        println(s"Timed out waiting for job '$jobId' to finish, checking its status")
        
        statusOf(jobId)
      }
    }
  }
  
  def runJob(pathToScript: Path, pathToUgerOutput: Path, isBulk: Boolean, jobName: String): SubmissionResult = {
    if (isBulk) {
      runBulkJobs(pathToScript, pathToUgerOutput, s"${jobName}BulkJobs", 1, 1, 1)
    } else {
      runSingleJob(pathToScript, pathToUgerOutput, s"${jobName}SingleJob")
    }
  }
  
  private def runSingleJob(
      pathToScript: Path, 
      pathToUgerOutput: Path,
      jobName: String): SubmissionResult = {
    
    withJobTemplate(session) { jt =>
    
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput")

      val jobId = session.runJob(jt)
    
      info(s"Job has been submitted with id $jobId")

      SingleJobSubmissionResult(jobId)
    }
  }

  private def runBulkJobs(
      pathToScript: Path, 
      pathToUgerOutput: Path, 
      jobName: String,
      start: Int, 
      end: Int, 
      incr: Int): SubmissionResult = {
    
    withJobTemplate(session) { jt =>

      jt.setNativeSpecification("-cwd -shell y -b n")
      jt.setRemoteCommand(pathToScript.toString)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput.${JobTemplate.PARAMETRIC_INDEX}")

      val jobIds = session.runBulkJobs(jt, start, end, incr).asScala
    
      info(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      BulkJobSubmissionResult(jobIds)
    }
  }

  private def withJobTemplate[A <: SubmissionResult](session: Session)(f: JobTemplate => A): SubmissionResult = {
    val jt = session.createJobTemplate
    
    try { f(jt) }
    catch {
      case e: DrmaaException => {
        error(s"Error: ${e.getMessage}", e)
        
        Failure(e)
      }
    }
    finally { session.deleteJobTemplate(jt) }
  }
}
