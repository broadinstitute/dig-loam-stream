package loamstream.drm

import scala.concurrent.duration._
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import loamstream.util.Loggable
import loamstream.util.Functions
import loamstream.util.RunResults
import loamstream.util.Processes
import loamstream.util.Tries
import loamstream.util.Loops
import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getExecutionNode(jobId: String): Option[String]

  def getQueue(jobId: String): Option[Queue]
  
  def getResourceUsage(jobId: String): Try[DrmResources]
}

object AccountingClient extends Loggable {
  protected[drm] type InvocationFn[A] = A => Try[RunResults] 
  
  protected[drm] val defaultDelayStart: Duration = 0.5.seconds
  protected[drm] val defaultDelayCap: Duration = 30.seconds
  
  protected[drm] def delaySequence(start: Duration, cap: Duration): Iterator[Duration] = {
    require(start gt 0.seconds)
    require(cap gt 0.seconds)
    
    Iterator.iterate(start)(_ * 2).map(_.min(cap))
  }
  
  protected[drm] def doRetries(
      binaryName: String,
      maxRetries: Int,
      delayStart: Duration,
      delayCap: Duration,
      delegateFn: InvocationFn[String]): InvocationFn[String] = Functions.memoize { jobId =>
        
    val maxRuns = maxRetries + 1
    
    def invokeBinary(): Try[RunResults] = delegateFn(jobId) match {
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
        val msg = {
          s"Invoking '$binaryName' for job with DRM id '$jobId' failed after $maxRuns runs; " +
           "execution stats won't be available"
        }
        
        debug(msg)

        Tries.failure(msg)
      }
    }
    
    result
  }
}
