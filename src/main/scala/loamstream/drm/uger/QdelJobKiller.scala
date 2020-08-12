package loamstream.drm.uger

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.util.Loggable
import loamstream.util.Users
import loamstream.drm.SessionSource
import loamstream.util.CommandInvoker

/**
 * @author clint
 * May 22, 2018
 */
final class QdelJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionSource: SessionSource) extends JobKiller with Loggable {
  
  override def killAllJobs(): Unit = {
    if(sessionSource.isInitialized) {
      commandInvoker.apply(()) match {
        case Success(runResults) => debug("Killed UGER jobs")
        case Failure(e) => warn(s"Error killing all UGER jobs: ${e.getMessage}", e)
      }
    } else {
      debug(s"No Uger session initialized; not killing jobs")
    }
  }
}

object QdelJobKiller extends JobKiller.Companion[QdelJobKiller]("qdel", new QdelJobKiller(_, _)) {
  override protected[drm] def makeTokens(
      sessionSource: SessionSource,
      actualExecutable: String, 
      username: String): Seq[String] = Seq(actualExecutable, "-u", username, "-si", sessionSource.getSession)
}
