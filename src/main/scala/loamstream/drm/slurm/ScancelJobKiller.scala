package loamstream.drm.slurm

import loamstream.drm.JobKiller
import loamstream.util.CommandInvoker
import scala.util.Failure
import loamstream.util.Loggable
import scala.util.Success
import loamstream.drm.SessionTracker

/**
 * @author clint
 * May 18, 2021
 */
final class ScancelJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionTracker: SessionTracker) extends JobKiller with Loggable {
  
  //TODO: Lots of shared code with QdelJobKiller
  
  override def killAllJobs(): Unit = {
    if(sessionTracker.nonEmpty) {
      commandInvoker.apply(()) match {
        case Success(runResults) => debug("Killed SLURM jobs")
        case Failure(e) => warn(s"Error killing all SLURM jobs: ${e.getMessage}", e)
      }
    } else {
      debug(s"No Slurm session initialized; not killing jobs")
    }
  }
}

object ScancelJobKiller extends JobKiller.Companion[ScancelJobKiller]("scancel", new ScancelJobKiller(_, _)) {
  override protected[drm] def makeTokens(
      sessionTracker: SessionTracker,
      actualExecutable: String, 
      username: String): Seq[String] = {
    
    val preamble = Seq(actualExecutable, "-u", username)
    
    def jobIdsPart = sessionTracker.taskArrayIdsSoFar
    
    if(sessionTracker.isEmpty) { preamble } 
    else { preamble ++ jobIdsPart } 
  }
}