package loamstream.uger

import loamstream.util.Loggable
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory
import scala.collection.JavaConverters._
import java.nio.file.Path
import org.ggf.drmaa.NoActiveSessionException

/**
 * Created on: 5/19/16 
 * @author Kaan Yuksel 
 * @author clint
 */
final class Drmaa extends Loggable {
  
  import Drmaa._
  
  private def runSingleJob(
      session: Session, 
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
      session: Session, 
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

  def runJob(pathToScript: Path, pathToUgerOutput: Path, isBulk: Boolean, jobName: String): SubmissionResult = {
    withSession { session =>
      if (isBulk) {
        runBulkJobs(session, pathToScript, pathToUgerOutput, s"${jobName}BulkJobs", 1, 3, 1)
      } else {
        runSingleJob(session, pathToScript, pathToUgerOutput, s"${jobName}SingleJob")
      }
    }
  }
  
  private def withJobTemplate[A <: SubmissionResult](session: Session)(f: JobTemplate => A): SubmissionResult = {
    val jt = session.createJobTemplate
    
    try { f(jt) }
    catch {
      case e: DrmaaException =>
        error(s"Error: ${e.getMessage}", e)

        Failure(e)
    }
    finally { session.deleteJobTemplate(jt) }
  }
  
  private def withSession[A](f: Session => A): A = {
    val session: Session = SessionFactory.getFactory.getSession

    try {
      session.init("")

      f(session)
    } finally {
      try { session.exit() }
      catch {
        //NB: session.exit() will fail if an exception was thrown by session.init().  In that case, just bail.
        case e: NoActiveSessionException => ()
      }
    }
  }
}

object Drmaa {
  sealed trait SubmissionResult
  
  final case class Failure(cause: Exception) extends SubmissionResult 
  
  final case class SingleJobSubmissionResult(jobId: String) extends SubmissionResult
  
  final case class BulkJobSubmissionResult(jobIds: Seq[Any]) extends SubmissionResult
}
