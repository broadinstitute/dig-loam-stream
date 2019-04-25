package loamstream.util

import scala.util.Try
import scala.concurrent.duration.Duration
import RetryingCommandInvoker.InvocationFn
import scala.util.Success

/**
 * @author clint
 * Apr 25, 2019
 */
final class RetryingCommandInvoker[A](
    maxRetries: Int,
    binaryName: String,
    delegateFn: InvocationFn[A],
    delayStart: Duration = Loops.Backoff.defaultDelayStart,
    delayCap: Duration = Loops.Backoff.defaultDelayCap) extends (InvocationFn[A]) with Loggable {
  
  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //bacct/qacct, more than necessary.
  //NB: If the operation fails, retry up to maxRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  override def apply(param: A): Try[RunResults] = runCommand(param)
  
  private val runCommand: InvocationFn[A] = {
    doRetries(
        binaryName = binaryName, 
        maxRetries = maxRetries, 
        delayStart = delayStart, 
        delayCap = delayCap, 
        delegateFn = delegateFn)
  }
  
  private def doRetries(
      binaryName: String,
      maxRetries: Int,
      delayStart: Duration,
      delayCap: Duration,
      delegateFn: InvocationFn[A]): InvocationFn[A] = Functions.memoize { param =>
        
    val maxRuns = maxRetries + 1
    
    def invokeBinary(): Try[RunResults] = delegateFn(param) match {
      case Success(r @ RunResults(executable, exitCode, _, _)) if r.isFailure => {
        val msg = s"Error invoking ${executable} (exit code ${exitCode})"
        
        r.logStdOutAndStdErr(s"$msg; output streams follow:", Loggable.Level.warn)
        
        Tries.failure(msg)
      }
      case attempt => attempt
    }
    
    val resultOpt = Loops.retryUntilSuccessWithBackoff(maxRuns, delayStart, delayCap) {
      invokeBinary()
    }
    
    val result: Try[RunResults] = resultOpt match {
      case Some(a) => Success(a)
      case _ => {
        val msg = s"Invoking '$binaryName' for with param '$param' failed after $maxRuns runs"
        
        debug(msg)

        Tries.failure(msg)
      }
    }
    
    result
  }
}

object RetryingCommandInvoker {
  type InvocationFn[A] = A => Try[RunResults]
}
