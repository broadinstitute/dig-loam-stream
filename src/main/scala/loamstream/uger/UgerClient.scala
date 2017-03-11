package loamstream.uger

import loamstream.oracle.uger.Queue
import loamstream.util.ValueBox

/**
 * @author clint
 * Mar 9, 2017
 */
trait UgerClient {
  def getExecutionNode(jobId: String): Option[String]
  
  def getQueue(jobId: String): Option[Queue]
}

object UgerClient {
  object QacctUgerClient {
    object Regexes {
      val qname = "qname\\s+(.+?)$".r
      val hostname = "hostname\\s+(.+?)$".r
    }
  }
  
  final class QacctUgerClient extends UgerClient {
    private[this] val qacctOutput: ValueBox[Map[String, Seq[String]]] = ValueBox(Map.empty)
    
    import QacctUgerClient._
    
    override def getExecutionNode(jobId: String): Option[String] = {
      val output = getQacctOutputFor(jobId)
      
      output.iterator.collect {
        case Regexes.hostname(node) => node
      }.toSeq.headOption
    }
  
    override def getQueue(jobId: String): Option[Queue] = {
      val output = getQacctOutputFor(jobId)
      
      output.iterator.collect {
        case Regexes.qname(queueName) => Queue.fromString(queueName)
      }.flatten.toSeq.headOption
    }
    
    private def makeTokens(jobId: String): Seq[String] = Seq("qacct", "-j", jobId)
    
    import scala.sys.process._
    
    private def getQacctOutputFor(jobId: String): Seq[String] = {
      qacctOutput.getAndUpdate { cache =>
        cache.get(jobId) match {
          case Some(output) => (cache, output)
          case None =>  {
            val tokens = makeTokens(jobId)
            
            val outputLines = tokens.lineStream.toIndexedSeq
            
            val newCache = cache + (jobId -> outputLines)
            
            (newCache, outputLines)
          }
        }
      }
    }
  }
}
 
