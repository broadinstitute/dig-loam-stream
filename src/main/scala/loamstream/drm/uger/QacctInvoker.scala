package loamstream.drm.uger

import loamstream.drm.AccountingCommandInvoker

/**
 * @author clint
 * Apr 23, 2019
 */
object QacctInvoker extends AccountingCommandInvoker.Companion {
  override def makeTokens(actualBinary: String = "qacct", jobId: String): Seq[String] = {
    Seq(actualBinary, "-j", jobId)
  }
}
