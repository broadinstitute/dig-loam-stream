package loamstream.drm.uger

import loamstream.drm.Poller
import loamstream.drm.DrmTaskId
import rx.lang.scala.Observable
import loamstream.util.Loggable
import scala.util.Try
import loamstream.util.RunResults
import loamstream.drm.DrmStatus
import QstatPoller.InvocationFn
import loamstream.util.Processes

/**
 * @author clint
 * Jul 15, 2020
 */
final class QstatPoller private[uger] (pollingFn: InvocationFn[Set[DrmTaskId]]) extends Poller with Loggable {
  override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    ???
  }
  
  override def stop(): Unit = ()
}

object QstatPoller extends Loggable {
  type InvocationFn[A] = A => Try[RunResults]
  
  private def makeTokens(actualExecutable: String, params: Set[DrmTaskId]): Seq[String] = ???
    
  def fromExecutable(actualExecutable: String = "bjobs"): QstatPoller = {
    def invocationFn(lsfJobIds: Set[DrmTaskId]): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable, lsfJobIds)
      
      trace(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    new QstatPoller(invocationFn)
  }
}
