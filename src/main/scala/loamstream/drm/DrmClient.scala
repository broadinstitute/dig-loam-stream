package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.Loggable

/**
 * @author clint
 * Mar 20, 2017
 */
final class DrmClient(
    drmaaClient: DrmaaClient, 
    accountingClient: AccountingClient) extends DrmaaClient with AccountingClient {
  
  import DrmClient.fillInAccountingFieldsIfNecessary
  
  override def getExecutionNode(jobId: String): Option[String] = accountingClient.getExecutionNode(jobId)
  
  override def getQueue(jobId: String): Option[Queue] = accountingClient.getQueue(jobId)
  
  override def submitJob(
      drmSettings: DrmSettings,
      drmConfig: DrmConfig,
      taskArray: DrmTaskArray): DrmSubmissionResult = {
    
    drmaaClient.submitJob(drmSettings, drmConfig, taskArray)
  }
    
  /**
   * Synchronously inspect the status of a job with the given ID
   *
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  override def statusOf(jobId: String): Try[DrmStatus] = {
    fillInAccountingFieldsIfNecessary(accountingClient, jobId) {
      drmaaClient.statusOf(jobId)
    }
  }

  /**
   * Wait (synchronously) for a job to complete.
   *
   * @param jobId the job ID, assigned by UGER, of the job to wait for
   * @param timeout how long to wait.  If timeout elapses and the job doesn't finish, try to determine the job's
   * status using statusOf()
   */
  override def waitFor(jobId: String, timeout: Duration): Try[DrmStatus] = {
    fillInAccountingFieldsIfNecessary(accountingClient, jobId) {
      drmaaClient.waitFor(jobId, timeout)
    }
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit = drmaaClient.stop()
  
  /**
   * Kill the job with the specified id, if the job is running.
   */
  override def killJob(jobId: String): Unit = drmaaClient.killJob(jobId)
  
  /**
   * Kill all jobs.
   */
  override def killAllJobs(): Unit = drmaaClient.killAllJobs()
}

object DrmClient extends Loggable {
  private[drm] def fillInAccountingFieldsIfNecessary(
      accountingClient: AccountingClient, 
      jobId: String)(attempt: Try[DrmStatus]): Try[DrmStatus] = {
    
    import loamstream.util.Classes.simpleNameOf
    
    for {
      drmStatus <- attempt
    } yield {
      if(drmStatus.isFinished) {
        debug(s"${simpleNameOf[DrmStatus]} is finished, determining execution node and queue: $drmStatus")
        
        val executionNode = accountingClient.getExecutionNode(jobId)
        val executionQueue = accountingClient.getQueue(jobId)
        
        val result = drmStatus.transformResources(_.withNode(executionNode).withQueue(executionQueue))
        
        debug(s"Invoked AccountingClient; new ${simpleNameOf[DrmStatus]} is: $result")
        
        result
      } else {
        debug(s"${simpleNameOf[DrmStatus]} is NOT finished, NOT determining execution node and queue: $drmStatus")
        
        drmStatus 
      }
    }
  }
}
