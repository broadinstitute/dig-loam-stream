package loamstream.uger

import scala.util.{ Failure, Success, Try }

import org.ggf.drmaa.InvalidJobException

import loamstream.util.Loggable
import loamstream.util.ObservableEnrichments
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler
import rx.lang.scala.observables.ConnectableObservable

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * Methods for monitoring jobs, returning Streams of JobStatuses
 */
object Jobs extends Loggable {
  private lazy val ioScheduler = IOScheduler()
  
  /**
   * Using the supplied Poller and polling frequency, produce an Observable stream of statuses for each job id
   * in a bunch of jobIds.  The statuses are the result of polling UGER via the supplied poller at the provided rate.
   * 
   * @param poller the Poller object to use to poll for job statuses
   * @param pollingFrequencyInHz the rate at which to poll
   * @param scheduler the RxScala Scheduler to run the polling operations on 
   * @param jobIds the ids of the jobs to monitor
   * @return a map of job ids to Observable streams of statuses for each job. The statuses are the result of polling 
   * UGER *synchronously* via the supplied poller at the supplied rate.
   */
  def monitor(poller: Poller, pollingFrequencyInHz: Double = 1.0, scheduler: Scheduler = ioScheduler)(
             jobIds: Iterable[String]): Map[String, Observable[JobStatus]] = {
    
    import scala.concurrent.duration._
    
    require(pollingFrequencyInHz != 0.0)
    require(pollingFrequencyInHz > 0.0 && pollingFrequencyInHz < 10.0)
    
    val period = (1 / pollingFrequencyInHz).seconds

    val ticks = Observable.interval(period, scheduler).share
    
    def poll(): Map[String, Try[JobStatus]] = poller.poll(jobIds)

    //NB: The call to .replay() is needed to allow us to 'demultiplex' the stream of poll results into
    //several streams, one for each job id, without worrying about missing any emitted values if any 
    //downstream subscribers subscribe "too late".
    //Note that we now need to connect() to pollResults before any ticks start emitting, and any polling
    //is done.
    val pollResults = ticks.map(_ => poll()).replay
    
    val byJobId: Map[String, Observable[Try[JobStatus]]] = demultiplex(jobIds, pollResults)

    val result = byJobId.map(unpackThenFilterThenLimit)
    
    //NB: connect() pollResults at the last possible moment.  This is necessary because pollResults is a 
    //ConnectableObservable, and won't start emitting values to subscribers until connect() is invoked.  
    //Calling .replay(), and then .connect() as late as possible makes it less likely (impossible?) that 
    //values emitted by pollResults will be missed by downstream subscribers.
    try { result }
    finally { pollResults.connect }
  }

  /**
   * Transforms the passed Observable by unpacking Trys, filtering out identical consecutive statuses, 
   * and completing when a 'terminal' status is seen.
   * 
   * @param jobStatusTuple a tuple of a jobId and an Observable stream of attempts at determining the status of the job
   * @return a tuple of the passed job id and an Observable stream of JobStatuses, with identical consecutive statuses 
   * filtered out, and that completes when a 'terminal' JobStatus is seen. (Done, Failed, etc; see 
   * JobStatus.isFinished)
   */
  private[uger] def unpackThenFilterThenLimit(
      jobStatusTuple: (String, Observable[Try[JobStatus]])): (String, Observable[JobStatus]) = {
    
    val (jobId, statusAttempts) = jobStatusTuple
    
    import ObservableEnrichments._
    import JobStatus.{DoneUndetermined, Undetermined}
    
    val statuses = statusAttempts.distinctUntilChanged.zipWithIndex.collect {
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
  private[uger] def demultiplex(
      jobIds: Iterable[String],
      multiplexed: ConnectableObservable[Map[String, Try[JobStatus]]]): Map[String, Observable[Try[JobStatus]]] = {
    
    val tuples = for {
      jobId <- jobIds
    } yield {
      val forJob = multiplexed.map(resultsById => resultsById(jobId))
        
      jobId -> forJob
    }
    
    tuples.toMap
  }
}
