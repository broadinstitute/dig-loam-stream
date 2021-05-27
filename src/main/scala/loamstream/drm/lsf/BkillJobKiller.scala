package loamstream.drm.lsf

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.drm.JobKiller
import loamstream.drm.SessionTracker
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.RunResults
import loamstream.drm.DrmSystem

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionSource: SessionTracker) extends JobKiller.ForCommand(DrmSystem.Lsf,
                                                                commandInvoker,
                                                                sessionSource) with Loggable
  
  /*override def killAllJobs(): Unit = {
    commandInvoker(()) match {
      case Success(runResults) => debug("Killed LSF jobs")
      case Failure(e) => warn(s"Error killing all LSF jobs: ${e.getMessage}", e)
    }
  }*/

object BkillJobKiller extends JobKiller.Companion[BkillJobKiller]("bkill", new BkillJobKiller(_, _)) {
  override protected[lsf] def makeTokens(
      sessionTracker: SessionTracker, 
      actualExecutable: String, 
      username: String): Seq[String] = Seq(actualExecutable, "-u", username, "0") //TODO: Use SessionTracker
}
