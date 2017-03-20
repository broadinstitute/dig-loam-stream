package loamstream.uger

import loamstream.conf.UgerConfig
import java.nio.file.Path
import scala.util.Try
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Mar 20, 2017
 */
final class UgerClient(
    drmaaClient: DrmaaClient, 
    accountingClient: AccountingClient) extends DrmaaClient with AccountingClient {
  
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
  override def statusOf(jobId: String): Try[UgerStatus] = drmaaClient.statusOf(jobId)

  /**
   * Wait (synchronously) for a job to complete.
 *
   * @param jobId the job ID, assigned by UGER, of the job to wait for
   * @param timeout how long to wait.  If timeout elapses and the job doesn't finish, try to determine the job's
   * status using statusOf()
   */
  override def waitFor(jobId: String, timeout: Duration): Try[UgerStatus] = {
    for {
      ugerStatus <- drmaaClient.waitFor(jobId, timeout)
    } yield {
      if(ugerStatus.isFinished) {
        val executionNode = accountingClient.getExecutionNode(jobId)
        val executionQueue = accountingClient.getQueue(jobId)
        
        ugerStatus.transformResources(_.copy(node = executionNode, queue = executionQueue))
      } else { ugerStatus }
    }
  }
  
  /**
   * Shut down this client and dispose of any DRMAA resources it has acquired (Sessions, etc)
   */
  override def stop(): Unit = drmaaClient.stop()
}
