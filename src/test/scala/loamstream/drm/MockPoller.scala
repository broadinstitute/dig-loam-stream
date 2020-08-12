package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.ValueBox
import loamstream.model.execute.Resources.DrmResources
import rx.lang.scala.Observable

/**
 * @author clint
 * date: Jul 6, 2016
 */
final case class MockPoller(private val toReturn: Map[DrmTaskId, Seq[Try[DrmStatus]]]) extends Poller {
  import loamstream.util.Maps.Implicits._
  
  private val remaining: ValueBox[Map[DrmTaskId, Seq[Try[DrmStatus]]]] = {
    ValueBox(toReturn.strictMapValues(_.init))
  }
  
  private val terminalStates = toReturn.map { case (jobId, statuses) => (jobId, statuses.last) }
  
  val params: ValueBox[Seq[(DrmTaskId, Duration)]] = ValueBox(Vector.empty)
  
  override def poll(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    val jobIdsToPoll = jobIds.toSet
    
    val resultsByTaskId = toReturn.filterKeys(jobIdsToPoll.contains)
    
    val pollingResults = for {
      (tid, attempts) <- resultsByTaskId.toSeq
      attempt <- attempts
    } yield (tid, attempt)
    
    Observable.from(pollingResults)
  }
  
  /*def statusOf(taskId: DrmTaskId): Try[DrmStatus] = waitFor(taskId, Duration.Zero)

  def waitFor(taskId: DrmTaskId, timeout: Duration): Try[DrmStatus] = {
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
  }*/

  override def stop(): Unit = ()
}
