package loamstream.drm.slurm



import loamstream.drm.DrmSystem
import loamstream.drm.JobKiller
import loamstream.drm.SessionTracker
import loamstream.util.CommandInvoker

/**
 * @author clint
 * May 18, 2021
 */
final class ScancelJobKiller(
    commandInvoker: CommandInvoker.Sync[Unit], 
    sessionTracker: SessionTracker) extends JobKiller.ForCommand(DrmSystem.Slurm, commandInvoker, sessionTracker)

object ScancelJobKiller extends JobKiller.Companion[ScancelJobKiller]("scancel", new ScancelJobKiller(_, _)) {
  override protected[drm] def makeTokens(
      sessionTracker: SessionTracker,
      actualExecutable: String, 
      username: String): Seq[String] = {
    
    val preamble = Seq(actualExecutable, "-u", username)
    
    def jobIdsPart = sessionTracker.drmTaskIdsSoFar.map(DrmSystem.Slurm.formatForScancel)
    
    if(sessionTracker.isEmpty) { preamble } 
    else { preamble ++ jobIdsPart } 
  }
}