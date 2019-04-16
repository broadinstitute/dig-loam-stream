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
  //NB: If qacct fails, retry up to ugerConfig.maxQacctRetries times, waiting 0.5, 1, 2, 4, ... up to 30s 
  //in between each one.
  private val qacctOutputForJobId: String => Seq[String] = {
    val doRetries: String => Seq[String] = { jobId =>
      val maxRuns = ugerConfig.maxQacctRetries + 1
      
      def invokeQacct(): Try[Seq[String]] = Try(qacctOutputForJobIdFn(jobId))
      
      val delays = delaySequence
      
      def delayIfFailure(attempt: Try[Seq[String]]): Try[Seq[String]] = {
        if(attempt.isFailure) {
          //TODO: evaluate whether or not blocking is ok. For now, it's expedient and doesn't seem to cause problems.
          Thread.sleep(delays.next().toMillis)
        }
        
        attempt
      }
      
      val attempts = Iterator.continually(invokeQacct()).take(maxRuns).map(delayIfFailure).dropWhile(_.isFailure)
      
      attempts.toStream.headOption match {
        case Some(Success(lines)) => lines
        case Some(Failure(e)) => {
          debug(s"Error invoking qacct for job with DRM id '$jobId'; execution stats won't be available: $e")

          trace(s"qacct invocation failure stack trace:", e)

          Seq.empty
        }
        case None => {
          val msg = {
            s"Invoking qacct for job with DRM id '$jobId' failed after $maxRuns runs; " +
             "execution stats won't be available"
          }
          
          debug(msg)

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
  
  private[uger] def delaySequence: Iterator[Duration] = {
    Iterator.iterate(0.5)(_ * 2).map(scala.math.min(30, _)).map(_.seconds)
  }
}
