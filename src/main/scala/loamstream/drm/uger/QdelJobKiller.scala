package loamstream.drm.uger

import scala.util.Failure
import scala.util.Success

import loamstream.drm.JobKiller
import loamstream.drm.SessionTracker
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.drm.DrmStatus
import loamstream.drm.DrmSystem

/**
 * @author clint
 * May 22, 2018
 */
final class QdelJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionTracker: SessionTracker) extends JobKiller.ForCommand(DrmSystem.Uger, commandInvoker, sessionTracker)

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
