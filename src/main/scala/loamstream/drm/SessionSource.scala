package loamstream.drm

import org.ggf.drmaa.Session
import loamstream.util.LogContext
import loamstream.util.Loggable
import scala.concurrent.duration.Duration
import loamstream.util.RetryingCommandInvoker
import loamstream.util.Loops
import scala.util.Try
import org.ggf.drmaa.SessionFactory
import loamstream.conf.DrmConfig

/**
 * @author clint
 * Jul 13, 2020
 */
trait SessionSource {
  def getSession: Session
}

object SessionSource extends Loggable {
  def default(drmConfig: DrmConfig): SessionSource = {
    Retrying(
        Default(GetSessionFunctions.drmaa),
        //TODO: have a dedicated knob for this
        drmConfig.maxJobSubmissionRetries)
  }
  
  final case class Default(fn: () => Session) extends SessionSource {
    override def getSession: Session = fn()
  }
  
  final case class Retrying(
      delegate: SessionSource,
      maxRetries: Int,
      delayStart: Duration = RetryingCommandInvoker.defaultDelayStart,
      delayCap: Duration = RetryingCommandInvoker.defaultDelayCap) extends SessionSource {
    
    override def getSession: Session = {
      val maxAttempts = maxRetries + 1
      
      val result = Loops.retryUntilSuccessWithBackoff(maxAttempts, delayStart, delayCap) {
        val attempt = Try(delegate.getSession)
        
        attempt.recover {
          case e => warn(s"Failed to get DRMAA Session, retrying if possible: ${e.getMessage}", e)
        }
        
        attempt
      }
      
      result match {
        case Some(s) => s
        case None => {
          throw new Exception(s"Couldn't get DRMAA Session after ${maxRetries} retries.  See previous log entries.") 
        }
      }
    }
  }
  
  object GetSessionFunctions {
    def drmaa: () => Session = { () =>
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
        case e: UnsatisfiedLinkError => {
          error(s"Please check if you are running on a system with UGER (e.g. Broad VM). " +
            s"Note that UGER is required if the configuration file specifies a 'uger { ... }' block.")
          throw e
        }
      }
    }
  }
}
