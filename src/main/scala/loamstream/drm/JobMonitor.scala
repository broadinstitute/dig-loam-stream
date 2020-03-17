package loamstream.drm

import scala.concurrent.duration.DurationDouble
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.ggf.drmaa.InvalidJobException

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

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * Methods for monitoring jobs, returning Streams of JobStatuses
 */
final class JobMonitor(
    scheduler: Scheduler = IOScheduler(),
    poller: Poller, 
    pollingFrequencyInHz: Double = 1.0) extends Terminable with Loggable {
  
  import JobMonitor.allFinished
  import JobMonitor.demultiplex
  import JobMonitor.getDrmStatusFor
  import JobMonitor.unpackThenFilterThenLimit
  
  private[this] val _isStopped: ValueBox[Boolean] = ValueBox(false)
  
  private[drm] def isStopped: Boolean = _isStopped.value
  
  private[this] val stopSignal: Subject[Any] = PublishSubject()
  
  /**
   * Stop all polling and prevent further polling by this JobMonitor.  Useful at app-shutdown-time. 
   */
  override def stop(): Unit = {
    _isStopped.update(true)
    
    stopSignal.onNext(())
    
    poller.stop()
  }
  
  /**
   * Using the supplied Poller and polling frequency, produce an Observable stream of statuses for each job id
   * in a bunch of jobIds.  The statuses are the result of polling UGER via the supplied poller at the provided rate.
   * 
   * @param poller the Poller object to use to poll for job statuses
   * @param pollingFrequencyInHz the rate at which to poll
   * @param scheduler the RxScala Scheduler to run the polling operations on 
   * @param jobIds the ids of the jobs to monitor
   * @return a map of job ids to Observable streams of statuses for each job. The statuses are the result of polling 
   * the DRM system *synchronously* via the supplied poller at the supplied rate.
   */
  def monitor(jobIds: Iterable[DrmTaskId]): Map[DrmTaskId, Observable[DrmStatus]] = {
    
    import scala.concurrent.duration._
    
    require(pollingFrequencyInHz != 0.0)
    require(pollingFrequencyInHz > 0.0 && pollingFrequencyInHz < 10.0)
    
    val period = (1 / pollingFrequencyInHz).seconds

    //A flag indicating whether or not we should keep polling for the given set of job ids.
    //TODO: find a better way to stop or shut down `ticks` :\
    val keepPolling: ValueBox[Boolean] = ValueBox(true)
    
    val ticks = Observable.interval(period, scheduler).onBackpressureDrop.takeUntil(stopSignal).share
    
    def poll(): Observable[(DrmTaskId, Try[DrmStatus])] = poller.poll(jobIds)
    
    def shouldContinue: Boolean = keepPolling.value
    
    import loamstream.util.Observables.Implicits._
    
    //NB: The call to .replay() is needed to allow us to 'demultiplex' the stream of poll results into
    //several streams, one for each job id, without worrying about missing any emitted values if any 
    //downstream subscribers subscribe "too late".
    //Note that we now need to connect() to pollResults before any ticks start emitting, and any polling
    //is done.
    //NB: Defensively use until(allFinished) to avoid situation where we keep polling even though all jobs are Done
    //NB: Defensively filter ticks based on isStopped and keepPolling flags to allow "forcibly" stopping polling, 
    //preventing cases where the app is shutting down (and releasing Drmaa resources like Sessions, etc) but 
    //polling, driven by `ticks` keeps going for a little while after.
    val pollResults = ticks
      .takeWhile(_ => shouldContinue)
      .flatMap(_ => poll())
      //.map(t =>  { println(s"%%%%%%%%% Pre-scan(): $t") ; t })
      .scan(Map.empty[DrmTaskId, Try[DrmStatus]])(_ + _).dropWhile(_.isEmpty)
      //.map(m =>  { println(s"%%%%%%%%% Post-scan(): $m") ; m })
      .takeUntil(allFinished(keepPolling, jobIds)(_))
      .replay
    
    val byJobId: Map[DrmTaskId, Observable[Try[DrmStatus]]] = demultiplex(jobIds, pollResults)

    val result = byJobId.map(unpackThenFilterThenLimit)
    
    //NB: connect() pollResults at the last possible moment.  This is necessary because pollResults is a 
    //ConnectableObservable, and won't start emitting values to subscribers until connect() is invoked.  
    //Calling .replay(), and then .connect() as late as possible makes it less likely (impossible?) that 
    //values emitted by pollResults will be missed by downstream subscribers.
    try { result }
    finally { pollResults.connect }
  }
}

object JobMonitor extends Loggable {
  /**
   * Transforms the passed Observable by unpacking Trys, filtering out identical consecutive statuses, 
   * and completing when a 'terminal' status is seen.
   * 
   * @param jobStatusTuple a tuple of a jobId and an Observable stream of attempts at determining the status of the job
   * @return a tuple of the passed job id and an Observable stream of JobStatuses, with identical consecutive statuses 
   * filtered out, and that completes when a 'terminal' JobStatus is seen. (Done, Failed, etc; see 
   * JobStatus.isFinished)
   */
  private[drm] def unpackThenFilterThenLimit(
      jobStatusTuple: (DrmTaskId, Observable[Try[DrmStatus]])): (DrmTaskId, Observable[DrmStatus]) = {
    
    val (jobId, statusAttempts) = jobStatusTuple
    
    import loamstream.drm.DrmStatus.{ DoneUndetermined, Undetermined }
    import loamstream.util.Observables.Implicits._
    
    val statuses: Observable[DrmStatus] = statusAttempts.distinctUntilChanged.zipWithIndex.collect {
      //NB: DRMAA might not report when jobs are done, say if it hasn't cached the final status of a job, so we 
      //assume that an 'unknown job' failure for a job we've previously inquired about successfully means the job 
      //is done, though we can't determine how such a job ended. :(
      case (Failure(e: InvalidJobException), i) if i > 0 => {
        //Appease scalastyle
        val msg = {
          s"Got InvalidJobException for job we've previously inquired about successfully; mapping to $DoneUndetermined"
        }
        
        warn(s"Job '$jobId': $msg")
        
        DoneUndetermined
      }
      //Any other polling failure leaves us unable to know the job's status
      case (Failure(e), _) => {
        warn(s"Job '$jobId': polling failed with a(n) ${e.getClass.getName}; mapping to $Undetermined", e)
        
        Undetermined
      }
      case (Success(status), _) => status
    }
    
    //'Finish' the result Observable when we get a 'final' status (done, failed, etc) from UGER.
    jobId -> statuses.until(_.isFinished)
  }
  
  /**
   * Turns an Observable stream of maps of job ids to status attempts into a map of job ids to Observable streams
   * of status attempts.  That is, take an Observable producing information about several jobs, and turn it into 
   * several Observables, each producing information about one job. 
   * 
   * @param jobIds the jobs we're polling for, and that are present in the maps produced by multiplexed
   * @param multiplexed an Observable stream of Maps from job ids to attempts at determining that job's status.
   * 
   * Note that multiplexed must be a ConnectableObservable for this to work. 
   */
  type PollingResults = Map[DrmTaskId, Try[DrmStatus]]
  
  private[drm] def demultiplex(
      taskIds: Iterable[DrmTaskId],
      multiplexed: ConnectableObservable[PollingResults]): Map[DrmTaskId, Observable[Try[DrmStatus]]] = {
    
    val tuples = for {
      taskId <- taskIds
    } yield {
      val forJob = multiplexed.flatMap(getDrmStatusFor(taskId))
        
      taskId -> forJob
    }
    
    tuples.toMap
  }
  
  private[drm] def getDrmStatusFor(taskId: DrmTaskId)(pollResultAttempts: PollingResults): Observable[Try[DrmStatus]] = {
    pollResultAttempts.get(taskId) match {
      case Some(pollResultAttempt) => Observable.just(pollResultAttempt)
      case None => {
        warn(s"No data found for job id '$taskId' when polling, forging onward")
        
        Observable.empty
      }
    }
  }
  
  private def unpack(tuple: (DrmTaskId, Try[DrmStatus])): (DrmTaskId, DrmStatus) = tuple match {
    case (taskId, attempt) => (taskId, attempt.getOrElse(DrmStatus.Undetermined))
  }
  
  private def allFinished(
      keepPollingFlag: ValueBox[Boolean],
      taskIds: Iterable[DrmTaskId])
     (pollResults: Map[DrmTaskId, Try[DrmStatus]]): Boolean = {
    
    def unpack(attempt: Try[DrmStatus]): DrmStatus = attempt.getOrElse(DrmStatus.Undetermined)
    
    val unpacked: Iterator[(DrmTaskId, DrmStatus)] = pollResults.iterator.map {
      case (tid, statusAttempt) => (tid, unpack(statusAttempt))
    }
    
    val allTasksPresent = taskIds.toSet == pollResults.keySet
    
    def allStatusesAreFinished = unpacked.forall { case (_, status) => status.isFinished }
    
    val result = allTasksPresent && allStatusesAreFinished
    
    if(result) {
      val idsString = pollResults.keys.toSeq.sorted.mkString(",")
      
      debug(s"Jobs are all finished: $idsString")
      
      //Note side effect :(
      keepPollingFlag.update(false)
    }
      
    result
  }
}
