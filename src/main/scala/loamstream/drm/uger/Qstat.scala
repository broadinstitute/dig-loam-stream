package loamstream.drm.uger

import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import scala.concurrent.ExecutionContext
import loamstream.drm.SessionSource

/**
 * @author clint
 * Jul 24, 2020
 */
object Qstat extends Loggable {
  type InvocationFn[A] = A => Try[RunResults]
  
  private[uger] def makeTokens(actualExecutable: String, sessionSource: SessionSource): Seq[String] = {
    Seq(actualExecutable, "-si", sessionSource.getSession)
  }
    
  final def commandInvoker(
      sessionSource: SessionSource,
      actualExecutable: String = "qstat")(implicit ec: ExecutionContext): CommandInvoker.Async[Unit] = {
    //Unit and ignored args are obviously a smell, but a more principled refactoring will have to wait.
    def invocationFn(ignored: Unit): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable, sessionSource)
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(tokens)()
    }
    
    new CommandInvoker.Async.JustOnce[Unit](actualExecutable, invocationFn)
  }
}
