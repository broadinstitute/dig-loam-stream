package loamstream.drm

import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.Tries
import loamstream.util.Loops
import rx.lang.scala.Observable
import loamstream.drm.DrmSubmissionResult.SubmissionSuccess
import scala.concurrent.duration.Duration
import loamstream.util.RetryingCommandInvoker
import loamstream.util.TimeUtils
import scala.util.Try
import loamstream.util.RunResults

/**
 * @author clint
 * Oct 17, 2017
 * 
 * A trait representing the notion of submitting jobs to Uger.  
 */
trait JobSubmitter extends Terminable {
  /**
   * Submit a batch of jobs to be run as a Uger task array (all packaged in one script).
   * @params jobs the jobs to submit
   * @param ugerSettings the Uger settings shared by all the jobs being submitted
   */
  def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult]
}

object JobSubmitter {
  
  final case class Retrying(
      delegate: JobSubmitter,
      maxRetries: Int,
      delayStart: Duration = RetryingCommandInvoker.defaultDelayStart,
      delayCap: Duration = RetryingCommandInvoker.defaultDelayCap) extends JobSubmitter {

    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
      val maxAttempts = maxRetries + 1
      
      import scala.concurrent.duration._
     
      def toDrmSubmissionResult(result: Option[Map[DrmTaskId, DrmJobWrapper]]): DrmSubmissionResult = result match {
        case Some(m) => SubmissionSuccess(m)
        case None => {
          import taskArray.{drmJobName, size} 
          
          Tries.failure(s"Submitting task array ${drmJobName} with ${size} jobs failed after ${maxAttempts} attempts")
        }
      }
      
      val resultObs = Loops.retryUntilSuccessWithBackoffAsync(maxAttempts, delayStart, delayCap) {
        delegate.submitJobs(drmSettings, taskArray)
      }
      
      resultObs.map(toDrmSubmissionResult)
    }
    
    override def stop(): Unit = delegate.stop()
  }
  
  object Retrying {
    def apply(delegate: JobSubmitter, drmConfig: DrmConfig): Retrying = {
      Retrying(delegate, drmConfig.maxJobSubmissionRetries)
    }
  }
  
  /**
   * @author clint
   * Oct 17, 2017
   * 
   * Default implementation of JobSubmitter; uses a DrmaaClient to submit jobs. 
   */
  final case class Drmaa(drmaaClient: DrmaaClient, drmConfig: DrmConfig) extends JobSubmitter {
    
    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
      Observable.just {
        TimeUtils.time(s"Submitting task array with ${taskArray.size} jobs") {
          drmaaClient.submitJob(drmSettings, drmConfig, taskArray)
        }
      }
    }
    
    override def stop(): Unit = drmaaClient.stop()
  }
  
  abstract class CommandSubmitterCompanion[A <: JobSubmitter](val drmSystem: DrmSystem) {
    type SubmissionFn = (drmSystem.Settings, DrmTaskArray) => Try[RunResults]
  }
}
