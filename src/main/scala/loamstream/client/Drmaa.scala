package loamstream.client

import loamstream.util.Loggable
import org.ggf.drmaa.DrmaaException
import org.ggf.drmaa.JobTemplate
import org.ggf.drmaa.Session
import org.ggf.drmaa.SessionFactory

import scala.collection.JavaConverters._

/**
 * Created on: 5/19/16 
 * @author Kaan Yuksel 
 * @author clint
 */
final class Drmaa extends Loggable {
  
  private def runSingleJob(
      session: Session, 
      pathToShapeItScript: String, 
      pathToUgerOutput: String,
      jobName: String): String = {
    
    withJobTemplate(session) { jt =>
    
      jt.setRemoteCommand(pathToShapeItScript)
      jt.setJobName(jobName)
      jt.setOutputPath(":" + pathToUgerOutput)

      val jobId = session.runJob(jt)
    
      info(s"Job has been submitted with id $jobId")

      jobId
    }
  }

  private def runBulkJobs(
      session: Session, 
      pathToShapeItScript: String, 
      pathToUgerOutput: String, 
      jobName: String,
      start: Int, 
      end: Int, 
      incr: Int): Seq[Any] = {
    
    withJobTemplate(session) { jt =>

      jt.setNativeSpecification("-cwd -shell y -b n")
      jt.setRemoteCommand(pathToShapeItScript)
      jt.setJobName(jobName)
      jt.setOutputPath(s":$pathToUgerOutput.${JobTemplate.PARAMETRIC_INDEX}")

      val jobIds = session.runBulkJobs(jt, start, end, incr).asScala
    
      info(s"Jobs have been submitted with ids ${jobIds.mkString(",")}")

      jobIds
    }
  }

  def runJob(pathToShapeItScript: String, pathToUgerOutput: String, isBulk: Boolean): Unit = {
    withSession { session =>
      try {
        if (isBulk) {
          runBulkJobs(session, pathToShapeItScript, pathToUgerOutput, "ShapeItBulkJobs", 1, 3, 1)
        }
        else {
          runSingleJob(session, pathToShapeItScript, pathToUgerOutput, "ShapeItSingleJob")
        }
      } catch {
        case e: DrmaaException => error(s"Error: ${e.getMessage}", e)
      }
    }
  }
  
  private def withJobTemplate[A](session: Session)(f: JobTemplate => A): A = {
    val jt = session.createJobTemplate
    
    try { f(jt) }
    finally { session.deleteJobTemplate(jt) }
  }
  
  private def withSession[A](f: Session => A): A = {
    val session: Session = SessionFactory.getFactory.getSession

    try {
      session.init("")

      f(session)
    } finally {
      session.exit()
    }
  }
}
