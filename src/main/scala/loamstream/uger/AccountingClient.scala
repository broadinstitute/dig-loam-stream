package loamstream.uger

import loamstream.util.Functions
import scala.util.control.NonFatal
import loamstream.util.Loggable
import scala.util.matching.Regex
import loamstream.util.Iterators

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some uger-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getExecutionNode(jobId: String): Option[String]

  def getQueue(jobId: String): Option[Queue]
}

object AccountingClient extends Loggable {
  /**
   * A UgerClient that retrieves metadata from whitespace-seperated key-value output, like what's
   * produced by `qacct`.  Parameterized on the actual method of retrieving this output given a
   * job id, to facilitate unit testing.
   */
  final class QacctUgerClient(val qacctOutputForJobIdFn: String => Seq[String]) extends AccountingClient {
    import QacctUgerClient._

    //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
    //qacct in the production case, more than necessary.
    //NB: Don't cache empty results, since this likely indicates a failure when invoking qaact, and we'd like to
    //be able to try again in that case.
    private val qacctOutputForJobId: String => Seq[String] = {
      val shouldCache: Seq[String] => Boolean = _.nonEmpty

      Functions.memoize(qacctOutputForJobIdFn, shouldCache)
    }

    protected def getQacctOutputFor(jobId: String): Seq[String] = qacctOutputForJobId(jobId)

    import Regexes.{hostname, qname}

    override def getExecutionNode(jobId: String): Option[String] = {
      val output = getQacctOutputFor(jobId)

      //NB: Empty hostname strings are considered invalid
      findField(output, hostname).map(_.trim).filter(_.nonEmpty)
    }

    override def getQueue(jobId: String): Option[Queue] = {
      val output = getQacctOutputFor(jobId)

      findField(output, qname).flatMap(Queue.fromString)
    }

    private def findField(fields: Seq[String], regex: Regex): Option[String] = {
      fields.collect { case regex(value) => value }.headOption
    }
  }

  object QacctUgerClient {
    private object Regexes {
      val qname = "qname\\s+(.+?)$".r
      val hostname = "hostname\\s+(.+?)$".r
    }
  }

  /**
   * Make a UgerAcct that will retrieve job metadata by running some executable, by default, `qacct`.
   */
  def useActualBinary(binaryName: String = "qacct"): QacctUgerClient = {
    import scala.sys.process._

    def getQacctOutputFor(jobId: String): Seq[String] = {
      val tokens = makeTokens(binaryName, jobId)

      val noopProcessLogger = ProcessLogger(_ => ())

      try {
        //NB: Use noopProcessLogger to suppress stderr output that would otherwise go to the console.
        //NB: Even with noopProcessLogger, all lines written to stdout will be available via lineStream.
        //NB: Use toIndexedSeq to eagerly consume stdout, effectively running the command synchronously.
        tokens.lineStream(noopProcessLogger).toIndexedSeq
      } catch {
        case NonFatal(e) => {
          debug(s"Error invoking '$binaryName'; execution node and queue won't be available: $e")
          trace(s"'$binaryName' invocation failure stack trace:", e)

          Seq.empty
        }
      }
    }

    new QacctUgerClient(getQacctOutputFor)
  }

  private[uger] def makeTokens(binaryName: String, jobId: String): Seq[String] = Seq(binaryName, "-j", jobId)
}

