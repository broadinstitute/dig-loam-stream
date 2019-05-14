package loamstream.drm.lsf

import scala.concurrent.duration.Duration
import loamstream.drm.AccountingClient
import loamstream.conf.LsfConfig
import loamstream.util.RunResults
import scala.util.Try
import loamstream.util.Processes
import loamstream.drm.AccountingCommandInvoker

/**
 * @author clint
 * Apr 23, 2019
 */
object BacctInvoker extends AccountingCommandInvoker.Companion {
  override def makeTokens(actualBinary: String = "bacct", jobId: String): Seq[String] = {
    Seq(
        actualBinary,
        //long format; displays everything we need, and lots we don't
        "-l", 
        //"unformatted" output; basically omits some line breaks to make parsing easier
        "-UF",
        jobId)
  }
}
