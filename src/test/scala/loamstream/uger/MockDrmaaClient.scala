package loamstream.uger

import scala.util.Try
import scala.concurrent.duration.Duration
import java.nio.file.Path
import loamstream.util.ValueBox

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class MockDrmaaClient(toReturn: Seq[Try[JobStatus]]) extends DrmaaClient {
  private val remaining: ValueBox[Seq[Try[JobStatus]]] = ValueBox(toReturn)
  
  val params: ValueBox[Seq[(String, Duration)]] = ValueBox(Vector.empty)
  
  override def submitJob(
      pathToScript: Path, 
      pathToUgerOutput: Path, 
      jobName: String,
      numTasks: Int): DrmaaClient.SubmissionResult = ???

  override def statusOf(jobId: String): Try[JobStatus] = ???

  override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = {
    params.mutate(_ :+ (jobId -> timeout))
    
    remaining.getAndUpdate(rem => (rem.tail, rem.head))
  }

  override def shutdown(): Unit = ()
}
  
object MockDrmaaClient {
  def apply(toReturn: Try[JobStatus]*): MockDrmaaClient = new MockDrmaaClient(toReturn)
}