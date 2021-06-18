package loamstream.drm.slurm

import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmSystem

/**
 * @author clint
 * Apr 23, 2019
 */
object SacctInvoker extends AccountingCommandInvoker.Companion[DrmTaskId] {
  override def makeTokens(actualBinary: String = "bacct", taskId: DrmTaskId): Seq[String] = {
    Seq(
        //Specify data columns we want 
        "--format",
        "MaxRSS,CPUTime,Start,End,Node",
        //Suppress header line
        "-n",
        //ask for memory quantities in gigs
        "--units=G",
        // output will be '|' delimited without a '|' at the end
        "-P",
        //Specify job/task id
        "-j",
        DrmSystem.Slurm.format(taskId))
  }
}