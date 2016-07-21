package loamstream.uger

import scala.util.Try
import scala.concurrent.duration.Duration
import java.nio.file.Path

/**
 * @author clint
 * date: Jul 6, 2016
 */
final class MockDrmaaClient(toReturn: Seq[Try[JobStatus]]) extends DrmaaClient {
  private var remaining = toReturn
  
  var params: Seq[(String, Duration)] = Vector.empty
  
  override def submitJob(
      pathToScript: Path, 
      pathToUgerOutput: Path, 
      jobName: String,
      numTasks: Int): DrmaaClient.SubmissionResult = ???

  override def statusOf(jobId: String): Try[JobStatus] = ???

  override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = {
    params :+= (jobId -> timeout)
    
    try { remaining.head }
    finally { remaining = remaining.tail }
  }

  override def shutdown(): Unit = ()
}
  
object MockDrmaaClient {
  def apply(toReturn: Try[JobStatus]*): MockDrmaaClient = new MockDrmaaClient(toReturn)
}