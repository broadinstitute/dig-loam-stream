package loamstream.uger

import java.nio.file.Path
import monix.reactive.Observable
import scala.concurrent.Future
import java.nio.file.Paths
import scala.util.Failure
import org.ggf.drmaa.InvalidJobException
import scala.util.Success
import loamstream.util.ObservableEnrichments

/**
 * @author clint
 * date: Jun 16, 2016
 */
object Jobs {
  def monitor(poller: Poller, frequencyInHz: Double = 1.0)(jobId: String): Observable[JobStatus] = {
    import scala.concurrent.duration._
    
    require(frequencyInHz != 0.0)
    
    val period = (1 / frequencyInHz).seconds
    
    val statusAttempts = for {
      _ <- Observable.interval(period)
      status <- Observable.fromFuture(poller.poll(jobId, period))
    } yield status
    
    val result = statusAttempts.distinctUntilChanged.zipWithIndex.collect { 
      //NB: DRMAA won't always report when jobs are done, so we assume that an 'unknown job' failure for a job
      //we've previously inquired about successfully means the job is done.  This means we can't determine how 
      //a job ended (success of failure), though.
      case (Failure(e: InvalidJobException), i) if i > 0 => JobStatus.Done
      //Any other polling failure leaves us unable to know the job's status
      case (Failure(_), _) => JobStatus.Undetermined
      case (Success(status), _) => status
    }
    
    import ObservableEnrichments._
    import monix.execution.Scheduler.Implicits.global
    
    result.takeUntil(_.isFinished)
  }
}