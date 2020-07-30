package loamstream.drm.lsf

import scala.concurrent.duration.Duration
import loamstream.drm.AccountingClient
import loamstream.conf.LsfConfig
import loamstream.util.RunResults
import scala.util.Try
import loamstream.util.Processes
import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Apr 23, 2019
 */
object BacctInvoker extends AccountingCommandInvoker.Companion[DrmTaskId] {
  override def makeTokens(actualBinary: String = "bacct", taskId: DrmTaskId): Seq[String] = {
    Seq(
        actualBinary,
        //long format; displays everything we need, and lots we don't
        "-l", 
        taskId.jobId)
  }
}
