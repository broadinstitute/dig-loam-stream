package loamstream.drm.uger

import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import scala.concurrent.ExecutionContext
import loamstream.drm.SessionSource
import loamstream.util.RateLimitedCache
import loamstream.util.LogContext

/**
 * @author clint
 * Jul 24, 2020
 */
object Qstat {
  type InvocationFn[A] = A => Try[RunResults]
  
  private[uger] def makeTokens(actualExecutable: String): Seq[String] = {
    Seq(actualExecutable)
  }
    
  final def commandInvoker(
      pollingFrequencyInHz: Double,
      actualExecutable: String = "qstat")
     (implicit ec: ExecutionContext, logCtx: LogContext): CommandInvoker.Async[Unit] = {
    
    //Unit and ignored args are obviously a smell, but a more principled refactoring will have to wait.
    def invocationFn(ignored: Unit): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable)
      
      logCtx.trace(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(tokens)()
    }
    
    import scala.concurrent.duration._
    
    val waitTime = (1.0 / pollingFrequencyInHz).seconds
    
    val cache = new RateLimitedCache(() => invocationFn(()), waitTime) 
    
    def cachedInvocationFn(ignored: Unit): Try[RunResults] = cache()
    
    new CommandInvoker.Async.JustOnce[Unit](actualExecutable, cachedInvocationFn)
  }
}
