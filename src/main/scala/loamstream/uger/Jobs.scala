package loamstream.uger

import scala.util.{ Failure, Success, Try }

import org.ggf.drmaa.InvalidJobException

import loamstream.util.Loggable
import loamstream.util.ObservableEnrichments
import loamstream.util.Maps
import rx.lang.scala.Observable
import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.IOScheduler

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
               import Maps.Implicits._
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
    
    val result = byJobId.strictMapValues(unpackFilterAndLimit)
    
    //NB: connect() pollResults at the last possible moment.
    try { result }
    finally { pollResults.connect }
  }

  /**
   * 
   */
  private[uger] def unpackFilterAndLimit(statusAttempts: Observable[Try[JobStatus]]): Observable[JobStatus] = {
    import ObservableEnrichments._
    val statuses = statusAttempts.distinctUntilChanged.zipWithIndex.collect {
      //NB: DRMAA won't always report when jobs are done, so we assume that an 'unknown job' failure for a job
      //we've previously inquired about successfully means the job is done.  This means we can't determine how 
      //a job ended (success of failure), though. :(
      case (Failure(e: InvalidJobException), i) if i > 0 => JobStatus.DoneUndetermined
      //Any other polling failure leaves us unable to know the job's status
      case (Failure(_), _) => JobStatus.Undetermined
      case (Success(status), _) => status
    }
    
    //'Finish' the result Observable when we get a 'final' status (done, failed, etc) from UGER.
    statuses.until(_.isFinished)
  }
  
  /**
   * Turns an Observable stream of maps of job ids to status attempts into a map of job ids to Observable streams
   * of status attempts.  That is, take an Observable producing information about several jobs, and turn it into 
   * several Observables, each producing information about one job.  
   */
  private[uger] def demultiplex(
      jobIds: Iterable[String],
      multiplexed: Observable[Map[String, Try[JobStatus]]]): Map[String, Observable[Try[JobStatus]]] = {
    
    val tuples = for {
      jobId <- jobIds
    } yield {
      val forJob = multiplexed.map(resultsById => resultsById(jobId))
        
      jobId -> forJob
    }
    
    tuples.toMap
  }
}
