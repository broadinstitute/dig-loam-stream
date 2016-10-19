package loamstream.apps

import java.nio.file.Path

import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NoStackTrace

import org.ggf.drmaa.DrmaaException
import org.scalatest.FunSuite

import loamstream.uger.DrmaaClient
import loamstream.uger.JobStatus

/**
 * @author clint
 * Oct 19, 2016
 */
final class DrmaaClientHelpersTest extends FunSuite {
  test("Drmaa client is properly shut down") {
    import DrmaaClientHelpersTest._
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      helpers.withClient(client => ())
      
      assert(helpers.mockClient.isShutdown === true)
    }
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      try {
        helpers.withClient(client => throw new Exception)
      } catch {
        case e: Exception => ()
      }
      
      assert(helpers.mockClient.isShutdown === true)
    }
    
    {
      val helpers = new MockHelpers
      
      assert(helpers.mockClient.isShutdown === false)
      
      try {
        helpers.withClient(client => throw new MockDrmaaException(""))
      } catch {
        case e: Exception => ()
      }
      
      assert(helpers.mockClient.isShutdown === true)
    }
  }
}

object DrmaaClientHelpersTest {
  final class MockDrmaaException(msg: String) extends DrmaaException(msg) with NoStackTrace
  
  final class MockHelpers extends DrmaaClientHelpers {
    val mockClient = new MockDrmaaClient
      
    override private[apps] val makeDrmaaClient: DrmaaClient = mockClient
  }

  final class MockDrmaaClient extends DrmaaClient {
    var isShutdown = false
      
    override def submitJob(
      pathToScript: Path, 
      pathToUgerOutput: Path, 
      jobName: String, 
      numTasks: Int = 1): DrmaaClient.SubmissionResult = ???
    
    override def statusOf(jobId: String): Try[JobStatus] = ???
  
    override def waitFor(jobId: String, timeout: Duration): Try[JobStatus] = ???
  
    override def shutdown(): Unit = {
      isShutdown = true
    }
  }
}