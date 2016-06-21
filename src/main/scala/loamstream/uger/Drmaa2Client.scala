package loamstream.uger

import org.ggf.drmaa2.MonitoringSession
import org.ggf.drmaa2.SessionManager
import org.ggf.drmaa2.JobInfo
import scala.util.Try
import loamstream.util.Tries
import scala.util.Success

/**
 * @author clint
 * date: Jun 21, 2016
 */
trait Drmaa2Client {
  def statusOf(jobId: String): Try[JobStatus]
}

object Drmaa2Client {
  final class Default extends Drmaa2Client {
    def statusOf(jobId: String): Try[JobStatus] = {
      //Oof, using the internal API seems like the only way :\ 
      val sessionManager: SessionManager = new com.sun.grid.drmaa2.SessionManagerImpl
      
      withMonitoringSession(sessionManager) { monitoringSession =>

        val jobInfo = makeJobInfo(jobId)
        
        import scala.collection.JavaConverters._
        
        val jobs = monitoringSession.getAllJobs(jobInfo).asScala
        
        val numResults = jobs.size
        
        if(numResults > 1) {
          Tries.failure(s"Expected one job with ID '$jobId', but found ${numResults}")
        } else {
          val jobState = jobs.headOption.map(_.getInfo.getJobState)
        
          jobState.map(JobStatus.fromJobState) match {
            case Some(status) => Success(status)
            case None => Tries.failure(s"Didn't find any jobs with ID '$jobId'")
          }
        }
      }
    }
  }
  
  /**
   * See https://github.com/troeger/drmaav2-mock/blob/master/drmaa2.h
   * and https://github.com/dgruber/drmaa2/blob/master/drmaa2.go
   * :( :(
   * 
     #define  DRMAA2_UNSET_STRING    NULL
     #define DRMAA2_UNSET_NUM -1
   * 
    func CreateJobInfo() (ji JobInfo) {
	    //strings are unset with ""
			ji.ExitStatus = C.DRMAA2_UNSET_NUM
			// slices are unset with nil
		  ji.Slots = C.DRMAA2_UNSET_NUM
		  // WallclockTime is unset with 0
		  ji.CPUTime = C.DRMAA2_UNSET_TIME
		  ji.State = Unset
		  // TODO Unset for Go Time type...
			return ji
		}
   */
  private def makeJobInfo(jobId: String): JobInfo = {
    val jobInfo = new JobInfo(null, -1L, null)
        
    jobInfo.setExitStatus(-1L)
    jobInfo.setSlots(-1L)
    jobInfo.setCpuTime(-1L)
    
    jobInfo.setJobId(jobId)
    
    jobInfo
  }
  
  private def withMonitoringSession[A](sessionManager: SessionManager)(f: MonitoringSession => A): A = {
    var monitoringSession: MonitoringSession = null
    
    try {
      monitoringSession = sessionManager.openMonitoringSession("")
      
      f(monitoringSession)
    } finally {
      if(monitoringSession != null) {
        sessionManager.closeMonitoringSession(monitoringSession)
      }
    }
  }
}
