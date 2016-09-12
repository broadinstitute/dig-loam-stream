package loamstream.uger

import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

import org.ggf.drmaa.InvalidJobException

import loamstream.util.ObservableEnrichments
import loamstream.util.TimeEnrichments._
import rx.lang.scala.Observable
import scala.concurrent.Future
import scala.util.Try

/**
 * @author clint
 * date: Jun 16, 2016
 * 
 * Methods for monitoring jobs, returning Streams of JobStatuses
 */
object Jobs {
  /**
   * Using the supplied Poller and polling frequency, produce an Observable stream of statuses for the job with the
   * given id.  The statuses are the result of polling UGER via the supplied poller at the provided rate.
   * 
   * @param poller the Poller object to use to poll for job statuses
   * @param pollingFrequencyInHz the rate at which to poll
   * @param jobId the id of the job to monitor
   * @return an Observable stream of statuses for the job with jobId. The statuses are the result of polling UGER 
   * *asynchronously* via the supplied poller at the supplied rate.
   */
  def monitor(poller: Poller, pollingFrequencyInHz: Double = 1.0)
             (jobId: String)
             (implicit context: ExecutionContext): Observable[JobStatus] = {
    
    import ObservableEnrichments._
    import scala.concurrent.duration._
    
    require(pollingFrequencyInHz != 0.0)
    require(pollingFrequencyInHz > 0.0 && pollingFrequencyInHz < 5.0)
    
    val period = (1 / pollingFrequencyInHz).seconds
    
    def poll(): Future[Try[JobStatus]] = time("Calling poll()") { poller.poll(jobId, period) }
    
    val statusAttempts = for {
      _ <- Observable.interval(period)
      status <- Observable.from(poll())
    } yield status
    
    val result = statusAttempts.distinctUntilChanged.zipWithIndex.collect { 
      //NB: DRMAA won't always report when jobs are done, so we assume that an 'unknown job' failure for a job
      //we've previously inquired about successfully means the job is done.  This means we can't determine how 
      //a job ended (success of failure), though. :(
      case (Failure(e: InvalidJobException), i) if i > 0 => JobStatus.Done
      //Any other polling failure leaves us unable to know the job's status
      case (Failure(_), _) => JobStatus.Undetermined
      case (Success(status), _) => status
    }
    
    //'Finish' the result Observable when we get a 'final' status (done, failed, etc) from UGER.
    result.until(_.isFinished)
  }
}