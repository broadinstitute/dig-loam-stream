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
    val qacctOutputForJobIdFn: String => Seq[String]) extends AccountingClient with Loggable {

  import QacctAccountingClient._

  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //qacct in the production case, more than necessary.
  //NB: Don't cache empty results, since this likely indicates a failure when invoking qaact, and we'd like to
  //be able to try again in that case.
  private val qacctOutputForJobId: String => Seq[String] = {
    val doRetries: String => Seq[String] = { jobId =>
      val maxRuns = ugerConfig.maxQacctRetries + 1
      
      def invokeQacct(): Try[Seq[String]] = Try(qacctOutputForJobIdFn(jobId))
      
      val attempt = Iterator.continually(invokeQacct()).take(maxRuns).dropWhile(_.isFailure).toStream
      
      attempt.headOption match {
        case Some(Success(lines)) => lines
        case Some(Failure(e)) => {
          debug(s"Error invoking qacct; execution stats won't be available: $e")

          trace(s"qacct invocation failure stack trace:", e)

          Seq.empty
        }
        case None => {
          debug(s"Invoking qacct failed after $maxRuns runs; execution stats won't be available")

          Seq.empty
        }
      }
    }
    
    Functions.memoize(doRetries)
  }

  protected def getQacctOutputFor(jobId: String): Seq[String] = qacctOutputForJobId(jobId)

  import Regexes.{ hostname, qname }

  override def getExecutionNode(jobId: String): Option[String] = {
    val output = getQacctOutputFor(jobId)

    //NB: Empty hostname strings are considered invalid
    findField(output, hostname).map(_.trim).filter(_.nonEmpty)
  }

  override def getQueue(jobId: String): Option[Queue] = {
    val output = getQacctOutputFor(jobId)

    findField(output, qname).map(_.trim).filter(_.nonEmpty).map(Queue(_))
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
   * Make a UgerAcct that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(ugerConfig: UgerConfig, binaryName: String = "qacct"): QacctAccountingClient = {
    import scala.sys.process._

    def invokeQacctFor(jobId: String): Seq[String] = {
      val tokens = makeTokens(binaryName, jobId)

      val noopProcessLogger = ProcessLogger(_ => ())

      //NB: Use noopProcessLogger to suppress stderr output that would otherwise go to the console.
      //NB: Even with noopProcessLogger, all lines written to stdout will be available via lineStream.
      //NB: Use toIndexedSeq to eagerly consume stdout, effectively running the command synchronously.
      tokens.lineStream(noopProcessLogger).toIndexedSeq
    }
    
    new QacctAccountingClient(ugerConfig, invokeQacctFor)
  }

  private[uger] def makeTokens(binaryName: String, jobId: String): Seq[String] = Seq(binaryName, "-j", jobId)
}
