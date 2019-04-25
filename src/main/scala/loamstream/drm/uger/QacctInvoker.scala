package loamstream.drm.uger

import scala.concurrent.duration.Duration
import scala.util.Try

import loamstream.conf.UgerConfig
import loamstream.drm.AccountingClient
import loamstream.util.Processes
import loamstream.util.RunResults


/**
 * @author clint
 * Apr 23, 2019
 */
final class QacctInvoker private[uger] (
    ugerConfig: UgerConfig,
    binaryName: String,
    delegateFn: String => Try[RunResults],
    delayStart: Duration = AccountingClient.defaultDelayStart,
    delayCap: Duration = AccountingClient.defaultDelayCap) extends (String => Try[RunResults]) {
  
  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //bacct in the production case, more than necessary.
  //NB: If bacct fails, retry up to lsfConfig.maxBacctRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  override def apply(jobId: String): Try[RunResults] = qacctOutputForJobId(jobId)
  
  private val qacctOutputForJobId: String => Try[RunResults] = {
    AccountingClient.doRetries(
        binaryName = binaryName, 
        maxRetries = ugerConfig.maxQacctRetries, 
        delayStart = delayStart, 
        delayCap = delayCap, 
        delegateFn = delegateFn)
  }
}

object QacctInvoker {
  /**
   * Make a QacctInvoker that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(ugerConfig: UgerConfig, binaryName: String = "qacct"): QacctInvoker = {
    def invokeQacctFor(jobId: String) = Processes.runSync(binaryName, makeTokens(binaryName, jobId))
    
    new QacctInvoker(ugerConfig, binaryName, invokeQacctFor)
  }
  
  private[uger] def makeTokens(actualBinary: String = "bacct", jobId: String): Seq[String] = {
    Seq(actualBinary, "-j", jobId)
  }
}
