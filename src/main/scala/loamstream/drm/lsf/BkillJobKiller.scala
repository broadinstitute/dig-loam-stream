package loamstream.drm.lsf

import scala.util.Try
import BkillJobKiller.InvocationFn 
import scala.util.Success
import scala.util.Failure
import loamstream.util.Loggable
import loamstream.util.RunResults
import loamstream.util.Processes

/**
 * @author clint
 * May 22, 2018
 */
final class BkillJobKiller(invocationFn: InvocationFn) extends Loggable {
  def killAllJobs(): Unit = {
    invocationFn() match {
      case Success(runResults) => debug("Killed LSF jobs")
      case Failure(e) => warn(s"Error killing all LSF jobs: ${e.getMessage}", e)
    }
  }
}

object BkillJobKiller {
  type InvocationFn = () => Try[RunResults]

  private def currentUserName: String = System.getProperty("user.name")
  
  private[lsf] def makeTokens(actualExecutable: String, username: String = currentUserName): Seq[String] = {
    Seq(actualExecutable, "-u", username, "0")
  }
  
  def fromExecutable(actualExecutable: String = "bkill"): BkillJobKiller = {
    val killJobs: InvocationFn = { () =>
      Processes.runSync(makeTokens(actualExecutable))
    }
    
    new BkillJobKiller(killJobs)
  }
}
