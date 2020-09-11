package loamstream.drm.uger

import loamstream.drm.AccountingCommandInvoker
import loamstream.drm.DrmTaskId
import rx.lang.scala.Scheduler
import scala.concurrent.ExecutionContext
import loamstream.util.CommandInvoker
import loamstream.util.Processes
import loamstream.util.RateLimitedCache
import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.ValueBox
import loamstream.conf.UgerConfig
import scala.concurrent.duration.Duration

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
        scheduler: Scheduler)(implicit ec: ExecutionContext): CommandInvoker.Async[String] = {

      val rateLimitedCache = RateLimitedCachedQacctInvoker.fromActualBinary(
          maxSize = ugerConfig.maxQacctCacheSize,
          maxAge = maxCacheAge,
          maxRetries = ugerConfig.maxRetries,
          binaryName = binaryName,
          scheduler = scheduler)
      
      rateLimitedCache.commandInvoker
    }
  }
}
