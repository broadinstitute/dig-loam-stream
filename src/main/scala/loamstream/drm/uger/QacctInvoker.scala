package loamstream.drm.uger

import scala.concurrent.duration.Duration

import loamstream.conf.UgerConfig
import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId
import loamstream.util.CommandInvoker
import monix.execution.Scheduler

/**
 * @author clint
 * Apr 23, 2019
 */
object QacctInvoker extends AccountingCommandInvoker.Companion[DrmTaskId] {
  override def makeTokens(actualBinary: String = "qacct", taskId: DrmTaskId): Seq[String] = {
    Seq(actualBinary, "-j", taskId.jobId, "-t", taskId.taskIndex.toString)
  }
  
  object ByTaskArray {
    def useActualBinary(
        binaryName: String,
        ugerConfig: UgerConfig,
        maxCacheAge: Duration,
        scheduler: Scheduler): CommandInvoker.Async[String] = {

      val rateLimitedCache = RateLimitedQacctInvoker.fromActualBinary(
          maxSize = ugerConfig.maxQacctCacheSize,
          maxAge = maxCacheAge,
          maxRetries = ugerConfig.maxRetries,
          binaryName = binaryName,
          scheduler = scheduler)
      
      rateLimitedCache.commandInvoker
    }
  }
}
