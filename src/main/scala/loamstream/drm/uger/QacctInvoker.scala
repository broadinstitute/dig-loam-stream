package loamstream.drm.uger

import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmTaskArray

/**
 * @author clint
 * Apr 23, 2019
 */
object QacctInvoker extends AccountingCommandInvoker.Companion {
  override def makeTokens(
      actualBinary: String = "qacct", 
      taskIdOrArray: Either[DrmTaskId, DrmTaskArray]): Seq[String] = taskIdOrArray match {
    
    case Left(taskId) => Seq(actualBinary, "-j", taskId.jobId, "-t", taskId.taskIndex.toString)
    case Right(taskArray) => Seq(actualBinary, "-j", taskArray.drmJobName)
  }
}
