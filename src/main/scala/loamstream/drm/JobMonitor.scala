package loamstream.drm

import scala.concurrent.duration.DurationDouble
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.Tries
import loamstream.util.ValueBox
import loamstream.util.Tuples
import scala.concurrent.duration.Duration
import monix.reactive.subjects.PublishSubject
import monix.reactive.Observable
import monix.execution.Scheduler
import monix.reactive.subjects.Subject
import scala.concurrent.duration.FiniteDuration
import monix.reactive.OverflowStrategy
import loamstream.util.Observables
import loamstream.model.jobs.DrmJobOracle
import scala.collection.compat._

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * Methods for monitoring jobs, returning Streams of JobStatuses
 * 
 * Using the supplied Poller and polling frequency, produce an Observable stream of statuses for each job id
 * in a bunch of jobIds.  The statuses are the result of polling the DRM system via the supplied poller at 
 * the provided rate.
 * 
 * @param scheduler the RxScala Scheduler to run the polling operations on
 * @param poller the Poller object to use to poll for job statuses
 * @param pollingFrequencyInHz the rate at which to poll
 */
final class JobMonitor(
    scheduler: Scheduler,
    poller: Poller, 
    pollingFrequencyInHz: Double = 1.0) extends Terminable with Loggable {
  
  require(pollingFrequencyInHz != 0.0)
  require(pollingFrequencyInHz > 0.0 && pollingFrequencyInHz < 10.0)
  
  import JobMonitor.PollingState
  
  private[this] val globalStopSignal: Subject[Unit, Unit] = PublishSubject()
  
  private val period: FiniteDuration = {
    import scala.concurrent.duration._
    
    (1 / pollingFrequencyInHz).seconds
  }
  
  private lazy val ticks: Observable[Long] = {
    Observable
      .interval(period)
      .subscribeOn(scheduler)
      .takeUntil(globalStopSignal)
      .asyncBoundary(Observables.defaultOverflowStrategy)
  }
  
  /**
   * Stop all polling and prevent further polling by this JobMonitor.  Useful at app-shutdown-time. 
   */
  override def stop(): Unit = {
    globalStopSignal.onNext(())
    
    globalStopSignal.onComplete()
    
    poller.stop()
  }
  
  /**
   * Produce an Observable stream of statuses for each task id in a bunch of task Ids.
   * 
   * @param taskIds the ids of the jobs to monitor
   * @return a map of job ids to Observable streams of statuses for each job. The statuses are the result of polling 
   * the DRM system *synchronously* via the supplied poller at the supplied rate.
   */
  def monitor(oracle: DrmJobOracle)(taskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, DrmStatus)] = {
    
    val distinctIdsBeingPolledFor = taskIds.to(Set)
    
    val stopSignal: Subject[Unit, Unit] = PublishSubject()
    
    import JobMonitor.unpack
    
    //NB: Mutability is lame, but it makes for much simpler code than making poll() recursive.
    val stillWaitingFor: ValueBox[Iterable[DrmTaskId]] = ValueBox(taskIds)
    
    def poll: Observable[(DrmTaskId, DrmStatus)] = poller.poll(oracle)(stillWaitingFor.value).map(unpack)
    
    val localTicks = ticks.takeUntil(stopSignal).asyncBoundary(Observables.defaultOverflowStrategy)
    
    val pollingResults = localTicks.flatMap(_ => poll)
    
    val z: PollingState = PollingState.initial(distinctIdsBeingPolledFor)
    
    import Observables.Implicits.ObservableOps
    
    val pollingStates = pollingResults.scan(z) { (state, idAndStatus) => 
      val s = state.handle(idAndStatus)
      
      stillWaitingFor := s.stillWaitingFor
      
      s
    }.until(_.allFinished(stopSignal))
    
    pollingStates.collect { case PollingState(_, Some(lastIdAndStatus)) => lastIdAndStatus }
  }
}

object JobMonitor extends Loggable {
  private[JobMonitor] final case class PollingState(
      stillWaitingFor: Set[DrmTaskId], 
      lastIdAndStatus: Option[(DrmTaskId, DrmStatus)]) {
    
    def allFinished(stopSignal: Subject[Unit, Unit]): Boolean = {
      val result = stillWaitingFor.isEmpty 
      
      if(result) {
        debug(s"Jobs are all finished: ${stillWaitingFor.to(Seq).sorted.mkString(",")}")
        
        //Note side effect :(
        stopSignal.onNext(())
        
        stopSignal.onComplete()
      }
        
      result
    }
    
    def handle(idAndStatus: (DrmTaskId, DrmStatus)): PollingState = {
      val (taskId, status) = idAndStatus
      
      val newWaitingFor = if(status.isFinished) stillWaitingFor - taskId else stillWaitingFor
      
      copy(stillWaitingFor = newWaitingFor, lastIdAndStatus = Some(idAndStatus))
    }
  }
      
  private[JobMonitor] object PollingState {
    def initial(taskIds: Set[DrmTaskId]): PollingState = PollingState(taskIds, None)
  }
  
  private def unpack(tuple: (DrmTaskId, Try[DrmStatus])): (DrmTaskId, DrmStatus) = tuple match {
    case (taskId, Success(status)) => (taskId, status)
    case (taskId, Failure(e)) => {
      warn(s"Job '${taskId}': polling failed with a(n) ${e.getClass.getName}; mapping to ${DrmStatus.Undetermined}", e)
      
      (taskId, DrmStatus.Undetermined)
    }
  }
}
