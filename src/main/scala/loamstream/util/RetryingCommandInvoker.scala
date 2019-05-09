package loamstream.util

import scala.util.Try
import scala.concurrent.duration.Duration
import RetryingCommandInvoker.InvocationFn
import RetryingCommandInvoker.SuccessfulInvocationFn
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Apr 25, 2019
 * 
 * Runs a function, delegateFn, up to maxRetries + 1 times, and returns the first Success, or a Failure otherwise.
 * 
 * Used to run command-line utils synchronously, retrying them if their exit codes are non-zero.  Waits greater
 * and greater periods after each failure, starting at delayStart and doubling after each failure, up to a max of
 * delayCap.  See Loops.Backoff.delaySequence and Loops.retryUntilSuccessWithBackoff . 
 */
final class RetryingCommandInvoker[A](
    maxRetries: Int,
    binaryName: String,
    delegateFn: InvocationFn[A],
    delayStart: Duration = RetryingCommandInvoker.defaultDelayStart,
    delayCap: Duration = RetryingCommandInvoker.defaultDelayCap) extends (SuccessfulInvocationFn[A]) with Loggable {
  
  //Memoize the function that retrieves the metadata, to avoid running something expensive, like invoking
  //bacct/qacct, more than necessary.
  //NB: If the operation fails, retry up to maxRetries times, by default waiting 
  //0.5, 1, 2, 4, ... up to 30s in between each one.
  override def apply(param: A): Try[RunResults.Successful] = runCommand(param)
  
  private val runCommand: SuccessfulInvocationFn[A] = {
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
      delegateFn: InvocationFn[A]): SuccessfulInvocationFn[A] = Functions.memoize { param =>
        
    val maxRuns = maxRetries + 1
    
    def invokeBinary(): Try[RunResults.Successful] = delegateFn(param) match {
      //Coerce invocations producing non-zero exit codes to Failures
      case Success(r: RunResults.Unsuccessful) => {
        val msg = s"Error invoking ${r.executable} (exit code ${r.exitCode})"
        
        r.logStdOutAndStdErr(s"$msg; output streams follow:", Loggable.Level.warn)
        
        Tries.failure(msg)
      }
      case Success(r: RunResults.Successful) => Success(r)
      //pass failure-failures and successful (0 exit code) invocations through
      case Failure(e) => Failure(e)
    }
    
    val resultOpt = Loops.retryUntilSuccessWithBackoff(maxRuns, delayStart, delayCap) {
      invokeBinary()
    }
    
    val result: Try[RunResults.Successful] = resultOpt match {
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
  
  type SuccessfulInvocationFn[A] = A => Try[RunResults.Successful]
  
  import scala.concurrent.duration._
  
  val defaultDelayStart: Duration = 0.5.seconds
  val defaultDelayCap: Duration = 30.seconds
}
