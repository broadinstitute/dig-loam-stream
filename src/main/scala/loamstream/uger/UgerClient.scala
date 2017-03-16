package loamstream.uger

import loamstream.oracle.uger.Queue
import loamstream.util.ValueBox
import loamstream.util.Functions
import scala.util.control.NonFatal
import loamstream.util.Loggable

/**
 * @author clint
 * Mar 9, 2017
 * 
 * An abstraction for getting some uger-specific metadata that can't currently be accessed via DRMAA 
 */
trait UgerClient {
  def getExecutionNode(jobId: String): Option[String]
  
  def getQueue(jobId: String): Option[Queue]
}

object UgerClient extends Loggable {
  /**
   * A UgerClient that retrieves metadata from whitespace-seperated key-value output, like what's
   * produced by `qacct`.  Parameterized on the actual method of retrieving this output given a 
   * job id, to facilitate unit testing.
   */
  final class QacctUgerClient(val qacctOutputForJobIdFn: String => Seq[String]) extends UgerClient {
    import QacctUgerClient._
    
    //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
    //qacct in the production case, more than necessary.
    private val qacctOutputForJobId: String => Seq[String] = Functions.memoize(qacctOutputForJobIdFn)
    
    protected def getQacctOutputFor(jobId: String): Seq[String] = qacctOutputForJobId(jobId)
    
    override def getExecutionNode(jobId: String): Option[String] = {
      val output = getQacctOutputFor(jobId)
      
      //NB: Empty hostname strings are considered invalid
      firstOption(output.iterator.collect { case Regexes.hostname(node) => node.trim }.filter(_.nonEmpty))
    }
  
    override def getQueue(jobId: String): Option[Queue] = {
      val output = getQacctOutputFor(jobId)
      
      firstOption(output.iterator.collect {
        case Regexes.qname(queueName) => Queue.fromString(queueName)
      }.flatten)
    }
    
    private def firstOption[A](iter: Iterator[A]): Option[A] = iter.toStream.headOption
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
         
      try {
        //Return all output lines
        tokens.lineStream.toIndexedSeq
      } catch {
        case NonFatal(e) => {
          warn(s"Error invoking '$binaryName'; execution node and queue won't be available.", e)
          
          Seq.empty
        }
      }
    }
    
    new QacctUgerClient(getQacctOutputFor)
  }

  private[uger] def makeTokens(binaryName: String, jobId: String): Seq[String] = Seq(binaryName, "-j", jobId)
}
 
