package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try

import loamstream.conf.DrmConfig
import loamstream.drm.DrmSubmissionResult.SubmissionSuccess
import loamstream.model.execute.DrmSettings
import loamstream.util.CommandInvoker
import loamstream.util.Loops
import loamstream.util.RunResults
import loamstream.util.Terminable
import loamstream.util.TimeUtils
import loamstream.util.Tries
import rx.lang.scala.Observable

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
      delayStart: Duration = CommandInvoker.Retrying.defaultDelayStart,
      delayCap: Duration = CommandInvoker.Retrying.defaultDelayCap) extends JobSubmitter {

    override def submitJobs(drmSettings: DrmSettings, taskArray: DrmTaskArray): Observable[DrmSubmissionResult] = {
      val maxAttempts = maxRetries + 1
      
     
      def toDrmSubmissionResult(result: Option[Map[DrmTaskId, DrmJobWrapper]]): DrmSubmissionResult = result match {
        case Some(m) => SubmissionSuccess(m)
        case None => {
          import taskArray.{ drmJobName, size } 
          
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
