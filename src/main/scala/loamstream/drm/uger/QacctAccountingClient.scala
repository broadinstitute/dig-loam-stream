package loamstream.drm.uger

import scala.util.control.NonFatal
import scala.util.matching.Regex
import loamstream.drm.AccountingClient
import loamstream.drm.Queue
import loamstream.util.Functions
import loamstream.util.Loggable
import scala.collection.Seq
import scala.sys.process.stringSeqToProcess
import loamstream.conf.UgerConfig
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.concurrent.duration._
import loamstream.util.RunResults
import loamstream.util.Processes

/**
 * @author clint
 * Mar 9, 2017
 *
 * An AccountingClient that retrieves metadata from whitespace-seperated key-value output, like what's
 * produced by `qacct`.  Parameterized on the actual method of retrieving this output given a
 * job id, to facilitate unit testing.
 */
final class QacctAccountingClient(
    ugerConfig: UgerConfig,
    binaryName: String,
    val qacctOutputForJobIdFn: AccountingClient.InvocationFn[String],
    delayStart: Duration = AccountingClient.defaultDelayStart,
    delayCap: Duration = AccountingClient.defaultDelayCap) extends AccountingClient with Loggable {

  import QacctAccountingClient._

  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //qacct in the production case, more than necessary.
  //NB: If qacct fails, retry up to ugerConfig.maxQacctRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  private val qacctOutputForJobId: AccountingClient.InvocationFn[String] = {
    AccountingClient.doRetries(
        binaryName = binaryName, 
        maxRetries = ugerConfig.maxQacctRetries, 
        delayStart = delayStart, 
        delayCap = delayCap, 
        delegateFn = qacctOutputForJobIdFn)
  }

  //NB: Failures will already have been logged, so we can drop any Failure(e) here.
  protected def getQacctOutputFor(jobId: String): Option[Seq[String]] = {
    qacctOutputForJobId(jobId).toOption.map(_.stdout)
  }

  import Regexes.{ hostname, qname }

  override def getExecutionNode(jobId: String): Option[String] = {
    for {
      output <- getQacctOutputFor(jobId)
      //NB: Empty hostname strings are considered invalid
      node <- findField(output, hostname).map(_.trim).filter(_.nonEmpty)
    } yield node
  }

  override def getQueue(jobId: String): Option[Queue] = {
    for {
      output <- getQacctOutputFor(jobId)
      //NB: Empty qname strings are considered invalid
      queue <- findField(output, qname).map(_.trim).filter(_.nonEmpty).map(Queue(_))
    } yield queue
  }

  private def findField(fields: Seq[String], regex: Regex): Option[String] = {
    fields.collect { case regex(value) => value }.headOption
  }
}

object QacctAccountingClient extends Loggable {
  private object Regexes {
    val qname = "qname\\s+(.+?)$".r
    val hostname = "hostname\\s+(.+?)$".r
  }

  /**
   * Make a QacctAccountingClient that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(ugerConfig: UgerConfig, binaryName: String = "qacct"): QacctAccountingClient = {
    def invokeQacctFor(jobId: String): Try[RunResults] = Processes.runSync(binaryName, makeTokens(binaryName, jobId))
    
    new QacctAccountingClient(ugerConfig, binaryName, invokeQacctFor)
  }

  private[uger] def makeTokens(binaryName: String, jobId: String): Seq[String] = Seq(binaryName, "-j", jobId)
}
