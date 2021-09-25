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

    //NB: We skip the --batch param and specify ids of job arrays, not individual tasks within job arrays.
    //hopefully this will keep the param list from getting too long.
    val preamble = Seq(
      actualExecutable, 
      //Only kill jobs from a specified user (in practice, who we're running as)
      "-u", username,
      //Quiet: don't report errors for already-completed jobs
      "--quiet"
      )
    
    //def jobIdsPart = sessionTracker.drmTaskIdsSoFar.map(DrmSystem.Slurm.format)
    def jobIdsPart = sessionTracker.drmTaskIdsSoFar.map(_.jobId).toSet
    
    if(sessionTracker.isEmpty) { Seq(actualExecutable) } //NB: Don't kill all jobs made by this user
    else { preamble ++ jobIdsPart } 
  }
}