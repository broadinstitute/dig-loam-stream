package loamstream.drm.uger

import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.CommandInvoker
import loamstream.util.Loggable
import loamstream.util.Processes
import scala.concurrent.ExecutionContext

/**
 * @author clint
 * Jul 24, 2020
 */
object Qstat extends Loggable {
  type InvocationFn[A] = A => Try[RunResults]
  
  private[uger] def makeTokens(actualExecutable: String): Seq[String] = {
    Seq(actualExecutable, "-si", Sessions.sessionId)
  }
    
  final def commandInvoker(actualExecutable: String = "qstat")(implicit ec: ExecutionContext): CommandInvoker[Unit] = {
    //Unit and ignored args are obviously a smell, but a more principled refactoring will have to wait.
    def invocationFn(ignored: Unit): Try[RunResults] = {
      val tokens = makeTokens(actualExecutable)
      
      debug(s"Invoking '$actualExecutable': '${tokens.mkString(" ")}'")
      
      Processes.runSync(actualExecutable, tokens)
    }
    
    new CommandInvoker.JustOnce[Unit](actualExecutable, invocationFn)
  }
}
