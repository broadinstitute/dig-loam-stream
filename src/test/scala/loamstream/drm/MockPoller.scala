package loamstream.drm

import scala.concurrent.duration.Duration
import scala.util.Try
import loamstream.conf.DrmConfig
import loamstream.model.execute.DrmSettings
import loamstream.util.ValueBox
import loamstream.model.execute.Resources.DrmResources
import monix.reactive.Observable

import loamstream.model.jobs.DrmJobOracle

import scala.collection.compat._

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
  
  private val isStoppedBox: ValueBox[Boolean] = ValueBox(false)
  
  def isStopped: Boolean = isStoppedBox.value
  
  override def poll(oracle: DrmJobOracle)(jobIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, Try[DrmStatus])] = {
    val jobIdsToPoll = jobIds.to(Set)
    
    val resultsByTaskId = toReturn.filterKeys(jobIdsToPoll.contains)
    
    val pollingResults = for {
      (tid, attempts) <- resultsByTaskId.to(Seq)
      attempt <- attempts
    } yield (tid, attempt)
    
    Observable.from(pollingResults)
  }
  
  override def stop(): Unit = isStoppedBox := true
}
