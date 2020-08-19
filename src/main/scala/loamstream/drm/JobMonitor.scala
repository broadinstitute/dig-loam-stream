package loamstream.drm

import scala.concurrent.duration.DurationDouble
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.Tries
import loamstream.util.ValueBox
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.Subject
import rx.lang.scala.observables.ConnectableObservable
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.subjects.PublishSubject
import loamstream.util.Tuples

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
  
  import JobMonitor.PollingState
  
  /**
   * Stop all polling and prevent further polling by this JobMonitor.  Useful at app-shutdown-time. 
   */
  override def stop(): Unit = poller.stop()
  
  /**
   * Produce an Observable stream of statuses for each task id in a bunch of task Ids.
   * 
   * @param taskIds the ids of the jobs to monitor
   * @return a map of job ids to Observable streams of statuses for each job. The statuses are the result of polling 
   * the DRM system *synchronously* via the supplied poller at the supplied rate.
   */
  def monitor(taskIds: Iterable[DrmTaskId]): Observable[(DrmTaskId, DrmStatus)] = {
    import scala.concurrent.duration._
    
    require(pollingFrequencyInHz != 0.0)
    require(pollingFrequencyInHz > 0.0 && pollingFrequencyInHz < 10.0)
    
    val period = (1 / pollingFrequencyInHz).seconds
    
    val distinctIdsBeingPolledFor = taskIds.toSet
    
    val stopSignal: Subject[Any] = PublishSubject()
    
    def poll: Observable[(DrmTaskId, Try[DrmStatus])] = poller.poll(taskIds)
    
    import JobMonitor.unpack
    
    val ticks = Observable.interval(period, scheduler).takeUntil(stopSignal).onBackpressureDrop
    
    val pollingResults = ticks.flatMap(_ => poll).map(JobMonitor.unpack)

    val z: PollingState = PollingState.initial(distinctIdsBeingPolledFor)
    
    val pollingStates = pollingResults.scan(z) { _.handle(_) }.takeUntil(_.allFinished(stopSignal))
    
    pollingStates.collect { case PollingState(_, Some(lastIdAndStatus)) => lastIdAndStatus }
  }
}

object JobMonitor extends Loggable {
  private[JobMonitor] final case class PollingState(
      stillWaitingFor: Set[DrmTaskId], 
      lastIdAndStatus: Option[(DrmTaskId, DrmStatus)]) {
    
    def allFinished(stopSignal: Subject[Any]): Boolean = {
      val result = stillWaitingFor.isEmpty 
      
      if(result) {
        debug(s"Jobs are all finished: ${stillWaitingFor.toSeq.sorted.mkString(",")}")
        
        //Note side effect :(
        stopSignal.onNext(())
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
