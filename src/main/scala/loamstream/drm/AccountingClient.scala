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
import loamstream.model.jobs.TerminationReason

/**
 * @author clint
 * Mar 9, 2017
 *
 * An abstraction for getting some environment-specific metadata that can't currently be accessed via DRMAA
 */
trait AccountingClient {
  def getResourceUsage(jobId: String): Try[DrmResources]
  
  def getTerminationReason(jobId: String): Try[Option[TerminationReason]]
  
  def getAccountingInfo(jobId: String): Try[AccountingInfo] = {
    for {
      rs <- getResourceUsage(jobId)
      tr <- getTerminationReason(jobId)
    } yield AccountingInfo(rs, tr)
  }
}

object AccountingClient extends Loggable {
  protected[drm] type InvocationFn[A] = A => Try[RunResults] 
  
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
