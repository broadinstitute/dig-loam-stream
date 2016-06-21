package loamstream.uger

import java.nio.file.Path
import monix.reactive.Observable
import scala.concurrent.Future
import java.nio.file.Paths

/**
 * @author clint
 * date: Jun 16, 2016
 */
object Jobs {
  def monitor(poller: Poller, frequencyInHz: Double = 1.0)(jobId: String): Observable[JobStatus] = {
    import scala.concurrent.duration._
    
    require(frequencyInHz != 0.0)
    
    val period = (1 / frequencyInHz).seconds
    
    for {
      _ <- Observable.interval(period)
      status <- Observable.fromFuture(poller.poll(jobId))
    } yield status
  }
}