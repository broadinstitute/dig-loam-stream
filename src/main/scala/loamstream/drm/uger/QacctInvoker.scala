package loamstream.drm.uger

import scala.concurrent.duration.Duration
import scala.util.Try

import loamstream.conf.UgerConfig
import loamstream.drm.AccountingClient
import loamstream.util.Processes
import loamstream.util.RunResults
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
