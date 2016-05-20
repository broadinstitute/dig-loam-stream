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
 */
class Drmaa extends Loggable {
  private def runSingleJob(session: Session, pathToShapeItScript: String, pathToUgerOutput: String,
                           jobName: String): String = {
    val jt: JobTemplate = session.createJobTemplate
    jt.setRemoteCommand(pathToShapeItScript)
    jt.setJobName(jobName)
    jt.setOutputPath(":" + pathToUgerOutput)

    val jobId = session.runJob(jt)
    info("Job has been submitted with id " + jobId)

    session.deleteJobTemplate(jt)

    jobId
  }

  private def runBulkJobs(session: Session, pathToShapeItScript: String, pathToUgerOutput: String, jobName: String,
                          start: Int, end: Int, incr: Int): List[Any] = {
    val jt: JobTemplate = session.createJobTemplate
    jt.setNativeSpecification("-cwd -shell y -b n")
    jt.setRemoteCommand(pathToShapeItScript)
    jt.setJobName(jobName)
    jt.setOutputPath(":" + pathToUgerOutput + "." + JobTemplate.PARAMETRIC_INDEX)

    val jobIds = session.runBulkJobs(jt, start, end, incr).asScala.toList
    info("Jobs have been submitted with ids " + jobIds.mkString(","))

    session.deleteJobTemplate(jt)

    jobIds
  }

  def runJob(args: Array[String]) {
    val factory: SessionFactory = SessionFactory.getFactory
    val session: Session = factory.getSession

    try {
      session.init("")

      val pathToShapeItScript = args(0)
      val pathToUgerOutput = args(1)
      val isBulk = args(2).toBoolean

      val drm: Drmaa = new Drmaa
      if (isBulk) {
        drm.runBulkJobs(session, pathToShapeItScript, pathToUgerOutput, "ShapeItBulkJobs", 1, 3, 1)
      }
      else {
        drm.runSingleJob(session, pathToShapeItScript, pathToUgerOutput, "ShapeItSingleJob")
      }

      session.exit()
    }
    catch {
      case e: DrmaaException => {
        error("Error: " + e.getMessage)
      }
    }
  }
}
