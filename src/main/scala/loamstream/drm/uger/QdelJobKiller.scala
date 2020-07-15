package loamstream.drm.uger

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.util.Loggable

/**
 * @author clint
 * May 22, 2018
 */
final class QdelJobKiller(invocationFn: JobKiller.InvocationFn) extends JobKiller with Loggable {
  override def killAllJobs(): Unit = {
    invocationFn() match {
      case Success(runResults) => debug("Killed UGER jobs")
      case Failure(e) => warn(s"Error killing all UGER jobs: ${e.getMessage}", e)
    }
  }
}

object QdelJobKiller extends JobKiller.Companion[QdelJobKiller]("qdel", new QdelJobKiller(_)) {
  override protected def makeTokens(actualExecutable: String, username: String): Seq[String] = {
    Seq(actualExecutable, "-u", username, "-si", Sessions.sessionId)
  }
}
