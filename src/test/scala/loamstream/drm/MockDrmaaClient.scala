package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.ValueBox

/**
 * @author clint
 * date: Jul 6, 2016
 */
final case class MockDrmaaClient(private val toReturn: Map[String, Seq[Try[DrmStatus]]]) extends DrmaaClient {
  import loamstream.util.Maps.Implicits._
  
  private val remaining: ValueBox[Map[String, Seq[Try[DrmStatus]]]] = {
    ValueBox(toReturn.strictMapValues(_.init))
  }
  
  private val terminalStates = toReturn.map { case (jobId, statuses) => (jobId, statuses.last) }
  
  val params: ValueBox[Seq[(String, Duration)]] = ValueBox(Vector.empty)
  
  override def submitJob(
      drmSettings: DrmSettings,
      drmConfig: DrmConfig,
      taskArray: DrmTaskArray): DrmSubmissionResult = ???

  override def statusOf(jobId: String): Try[DrmStatus] = waitFor(jobId, Duration.Zero)

  override def waitFor(jobId: String, timeout: Duration): Try[DrmStatus] = {
    params.mutate(_ :+ (jobId -> timeout))
    
    remaining.getAndUpdate { leftToReturn => 
      val statuses = leftToReturn.get(jobId).filter(_.nonEmpty)
      
      val (nextStatus, remainingStatuses) = statuses match {
        case Some(sts) => (sts.head, sts.tail)
        case None => (terminalStates(jobId), Nil)
      }
      
      val nextMap = leftToReturn.updated(jobId, remainingStatuses)
      
      nextMap -> nextStatus
    }
  }

  override def stop(): Unit = ()
  
  override def killJob(jobId: String): Unit = ???
  
  override def killAllJobs(): Unit = ???
}
