package loamstream.drm.lsf

import scala.concurrent.duration.Duration
import loamstream.drm.AccountingClient
import loamstream.conf.LsfConfig
import loamstream.util.RunResults
import scala.util.Try
import loamstream.util.Processes

/**
 * @author clint
 * Apr 23, 2019
 */
final class BacctInvoker private[lsf] (
    lsfConfig: LsfConfig,
    binaryName: String,
    delegateFn: String => Try[RunResults],
    delayStart: Duration = AccountingClient.defaultDelayStart,
    delayCap: Duration = AccountingClient.defaultDelayCap) extends (String => Try[RunResults]) {
  
  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //bacct in the production case, more than necessary.
  //NB: If bacct fails, retry up to lsfConfig.maxBacctRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  override def apply(jobId: String): Try[RunResults] = bacctOutputForJobId(jobId)
  
  private val bacctOutputForJobId: String => Try[RunResults] = {
    AccountingClient.doRetries(
        binaryName = binaryName, 
        maxRetries = lsfConfig.maxBacctRetries, 
        delayStart = delayStart, 
        delayCap = delayCap, 
        delegateFn = delegateFn)
  }
}

object BacctInvoker {
  /**
   * Make a BacctInvoker that will retrieve job metadata by running some executable, by default, `bacct`.
   */
  def useActualBinary(lsfConfig: LsfConfig, binaryName: String = "bacct"): BacctInvoker = {
    def invokeBacctFor(jobId: String) = Processes.runSync(binaryName, makeTokens(binaryName, jobId))
    
    new BacctInvoker(lsfConfig, binaryName, invokeBacctFor)
  }
  
  private[lsf] def makeTokens(actualBinary: String = "bacct", jobId: String): Seq[String] = {
    Seq(
        actualBinary,
        //long format; displays everything we need, and lots we don't
        "-l", 
        //"unformatted" output; basically omits some line breaks to make parsing easier
        "-UF",
        jobId)
  }
}
