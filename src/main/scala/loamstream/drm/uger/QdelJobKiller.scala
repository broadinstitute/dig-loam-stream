package loamstream.drm.uger

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.drm.SessionTracker
import loamstream.util.CommandInvoker
import loamstream.util.Loggable

/**
 * @author clint
 * May 22, 2018
 */
final class QdelJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionTracker: SessionTracker) extends JobKiller with Loggable {
  
  override def killAllJobs(): Unit = {
    if(sessionTracker.nonEmpty) {
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
      sessionTracker: SessionTracker,
      actualExecutable: String, 
      username: String): Seq[String] = {
    
    val preamble = Seq(actualExecutable, "-u", username)
    
    def jobIdsPart = sessionTracker.taskArrayIdsSoFar.mkString(",")
    
    if(sessionTracker.isEmpty) { preamble } 
    else { preamble :+ jobIdsPart } 
  }
}
