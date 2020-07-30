package loamstream.drm.uger

import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Apr 23, 2019
 */
object QacctInvoker extends AccountingCommandInvoker.Companion[DrmTaskId] {
  override def makeTokens(actualBinary: String = "qacct", taskId: DrmTaskId): Seq[String] = {
    Seq(actualBinary, "-j", taskId.jobId, "-t", taskId.taskIndex.toString)
  }
  
  object ByTaskArray extends AccountingCommandInvoker.Companion[String] {
    override def makeTokens(actualBinary: String = "qacct", jobNumber: String): Seq[String] = {
      Seq(actualBinary, "-j", jobNumber)
    }
  }
}
