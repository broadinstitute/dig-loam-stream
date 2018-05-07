package loamstream.apps



import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.control.NoStackTrace

import org.ggf.drmaa.DrmaaException
import org.scalatest.FunSuite

import loamstream.conf.UgerConfig
import loamstream.model.execute.UgerSettings
import loamstream.uger.DrmaaClient
import loamstream.uger.DrmStatus
import loamstream.uger.UgerTaskArray

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
      
    override private[apps] def makeDrmaaClient: DrmaaClient = mockClient
  }

  final class MockDrmaaClient extends DrmaaClient {
    var isShutdown = false
      
    override def submitJob(
        ugerSettings: UgerSettings,
        ugerConfig: UgerConfig,
        taskArray: UgerTaskArray): DrmaaClient.SubmissionResult = ???
    
    override def statusOf(jobId: String): Try[DrmStatus] = ???
  
    override def waitFor(jobId: String, timeout: Duration): Try[DrmStatus] = ???
  
    override def stop(): Unit = {
      isShutdown = true
    }
    
    override def killJob(jobId: String): Unit = ???
  
    override def killAllJobs(): Unit = ???
  }
}
