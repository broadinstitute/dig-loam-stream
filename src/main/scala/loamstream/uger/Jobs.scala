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
  def poll(jobId: String): Future[JobStatus] = ???
  
  private def doSubmit(script: Path, workDir: Path): Future[String] = {
    val drmaaClient = new Drmaa
    
    //TODO
    val ugerOutput = Paths.get("/tmp/ugerOuput")
    
    drmaaClient.runJob(script, ugerOutput, true, "foobar")
    
    ???
  }
  
  def submit(script: Path, workDir: Path): Observable[JobStatus] = {
    import scala.concurrent.duration._
    
    val jobIdObservable: Observable[String] = Observable.fromFuture(doSubmit(script, workDir))
    
    for {
      jobId <- jobIdObservable
      _ <- Observable.interval(1.second)
      status <- Observable.fromFuture(poll(jobId))
    } yield status
  }
}