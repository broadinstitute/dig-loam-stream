package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.ValueBox
import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * date: Jul 6, 2016
 */
final case class MockDrmaaClient(private val toReturn: Map[DrmTaskId, Seq[Try[DrmStatus]]]) extends DrmaaClient {
  import loamstream.util.Maps.Implicits._
  
  private val remaining: ValueBox[Map[DrmTaskId, Seq[Try[DrmStatus]]]] = {
    ValueBox(toReturn.strictMapValues(_.init))
  }
  
  private val terminalStates = toReturn.map { case (jobId, statuses) => (jobId, statuses.last) }
  
  val params: ValueBox[Seq[(DrmTaskId, Duration)]] = ValueBox(Vector.empty)
  
  override def submitJob(
      drmSettings: DrmSettings,
      drmConfig: DrmConfig,
      taskArray: DrmTaskArray): DrmSubmissionResult = ???

  override def statusOf(taskId: DrmTaskId): Try[DrmStatus] = waitFor(taskId, Duration.Zero)

  override def waitFor(taskId: DrmTaskId, timeout: Duration): Try[DrmStatus] = {
    params.mutate(_ :+ (taskId -> timeout))
    
    remaining.getAndUpdate { leftToReturn => 
      val statuses = leftToReturn.get(taskId).filter(_.nonEmpty)
      
      val (nextStatus, remainingStatuses) = statuses match {
        case Some(sts) => (sts.head, sts.tail)
        case None => (terminalStates(taskId), Nil)
      }
      
      val nextMap = leftToReturn.updated(taskId, remainingStatuses)
      
      nextMap -> nextStatus
    }
  }

  override def stop(): Unit = ()
  
  override def killJob(taskId: DrmTaskId): Unit = ???
  
  override def killAllJobs(): Unit = ???
}
