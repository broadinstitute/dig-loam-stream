package loamstream.drm.lsf

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.util.Loggable
import loamstream.drm.SessionSource
import loamstream.util.CommandInvoker
import scala.util.Try
import loamstream.util.RunResults

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionSource: SessionSource = SessionSource.Noop) extends JobKiller with Loggable {
  
  override def killAllJobs(): Unit = {
    commandInvoker(()) match {
      case Success(runResults) => debug("Killed LSF jobs")
      case Failure(e) => warn(s"Error killing all LSF jobs: ${e.getMessage}", e)
    }
  }
}

object BkillJobKiller extends JobKiller.Companion[BkillJobKiller]("bkill", new BkillJobKiller(_, _)) {
  private[lsf] def apply(fn: () => Try[RunResults]): BkillJobKiller = {
    new BkillJobKiller(new CommandInvoker.Sync.JustOnce[Unit]("bkill", _ => fn()))
  }
  
  override protected[lsf] def makeTokens(
      sessionSource: SessionSource, 
      actualExecutable: String, 
      username: String): Seq[String] = Seq(actualExecutable, "-u", username, "0")
}
