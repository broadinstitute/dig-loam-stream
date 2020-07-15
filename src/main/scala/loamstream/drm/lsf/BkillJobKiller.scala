package loamstream.drm.lsf

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.util.Loggable

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKiller(invocationFn: JobKiller.InvocationFn) extends JobKiller with Loggable {
  override def killAllJobs(): Unit = {
    invocationFn() match {
      case Success(runResults) => debug("Killed LSF jobs")
      case Failure(e) => warn(s"Error killing all LSF jobs: ${e.getMessage}", e)
    }
  }
}

object BkillJobKiller extends JobKiller.Companion[BkillJobKiller]("bkill", new BkillJobKiller(_)) {
  override protected[lsf] def makeTokens(actualExecutable: String, username: String): Seq[String] = {
    Seq(actualExecutable, "-u", username, "0")
  }
}
