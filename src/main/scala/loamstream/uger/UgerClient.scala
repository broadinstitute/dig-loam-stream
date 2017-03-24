package loamstream.uger

import loamstream.conf.UgerConfig
import java.nio.file.Path
import scala.util.Try
import scala.concurrent.duration.Duration
import loamstream.util.Loggable
import scala.util.Success

/**
 * @author clint
 * Mar 20, 2017
 */
final class UgerClient(
    drmaaClient: DrmaaClient, 
    accountingClient: AccountingClient) extends DrmaaClient with AccountingClient {
  
  import UgerClient.fillInAccountingFieldsIfNecessary
  
  override def getExecutionNode(jobId: String): Option[String] = accountingClient.getExecutionNode(jobId)
  
  override def getQueue(jobId: String): Option[Queue] = accountingClient.getQueue(jobId)
  
  override def submitJob(
      ugerConfig: UgerConfig,
      pathToScript: Path,
      jobName: String,
      numTasks: Int = 1): DrmaaClient.SubmissionResult = {
    
    drmaaClient.submitJob(ugerConfig, pathToScript, jobName, numTasks)
  }
    
  /**
   * Synchronously inspect the status of a job with the given ID
   *
   * @param jobId the job ID, assigned by UGER, to inquire about
   * @return a Try, since inquiring might fail
   */
  override def statusOf(jobId: String): Try[UgerStatus] = {
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
  override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = {
    fillInAccountingFieldsIfNecessary(accountingClient, jobId) {
      drmaaClient.waitFor(jobId, timeout)
    }
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit = drmaaClient.stop()
}

object UgerClient extends Loggable {
  private[uger] def fillInAccountingFieldsIfNecessary(
      accountingClient: AccountingClient, 
      jobId: String)(attempt: Try[UgerStatus]): Try[UgerStatus] = {
    
    for {
      ugerStatus <- attempt
    } yield {
      if(ugerStatus.isFinished) {
        debug(s"UgerStatus is finished, determining execution node and queue: $ugerStatus")
        
        val executionNode = accountingClient.getExecutionNode(jobId)
        val executionQueue = accountingClient.getQueue(jobId)
        
        val result = ugerStatus.transformResources(_.copy(node = executionNode, queue = executionQueue))
        
        debug(s"Invoked AccountingClient; new UgerStatus is: $result")
        
        result
      } else {
        debug(s"UgerStatus is NOT finished, NOT determining execution node and queue: $ugerStatus")
        
        ugerStatus 
      }
    }
  }
}
