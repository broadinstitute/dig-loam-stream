package loamstream.drm.uger

import java.time.LocalDateTime
import java.time.ZonedDateTime

import scala.util.Try

import org.scalatest.FunSuite

import loamstream.conf.UgerConfig
import loamstream.drm.Queue
import loamstream.util.RunResults
import loamstream.util.Tries

/**
 * @author clint
 * Mar 15, 2017
 */
final class QacctAccountingClientTest extends FunSuite {

  import QacctTestHelpers.actualQacctOutput
  import QacctTestHelpers.expectedResources
  import QacctTestHelpers.successfulRun
  import scala.concurrent.duration._

  val newline = scala.util.Properties.lineSeparator
  
  test("toInstant - problematic dates") {
    val localTzOffset = ZonedDateTime.now.getOffset
    
    def doTest(ugerFormat: String, expectedInIsoFormat: String): Unit = {
      val expected = LocalDateTime.parse(expectedInIsoFormat).toInstant(localTzOffset)
      
      val parsed = QacctAccountingClient.toInstant("start")(ugerFormat)
      
      assert(parsed.get === expected)
    }
    
    //NB: One is DST, one isn't :\
    doTest("04/25/2019 14:20:36.264", "2019-04-25T14:20:36.264")
    doTest("03/06/2017 17:49:50.505", "2017-03-06T18:49:50.505")
  }
  
  test("getResourceUsage - accounting client fails") {
    val ugerConfig = UgerConfig(maxQacctRetries = 0)
    
    val mockClient = new MockQacctAccountingClient(_ => Tries.failure("blarg"), ugerConfig)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    assert(mockClient.getResourceUsage(jobId).isFailure)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
  }
  
  test("getResourceUsage - happy path") {
    val ugerConfig = UgerConfig()
    
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")
    
    val qacctOutput = actualQacctOutput(Some(expectedQueue), Some(expectedNode))
    
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = qacctOutput), ugerConfig)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    val actualResources = mockClient.getResourceUsage(jobId).get
    
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
    
    val expectedRawData = actualQacctOutput(Option(expectedQueue), Option(expectedNode)).mkString(newline)
    
    assert(actualResources === expectedResources(expectedRawData, expectedNode, expectedQueue))
  }
  
  test("retries - never works") {
    val maxRuns = 6
    
    val ugerConfig = UgerConfig(maxQacctRetries = maxRuns - 1)
    
    val mockClient = new MockQacctAccountingClient(_ => Tries.failure("blarg"), ugerConfig, 0.001.seconds, 0.5.seconds)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    assert(mockClient.getResourceUsage(jobId).isFailure)
    
    //Should have retried 
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
    
    assert(mockClient.getResourceUsage(jobId).isFailure)
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetResourceUsageInvoked === 2)
  }
  
  test("retries - works after 2 failures") {
    val maxRuns = 5
    
    val ugerConfig = UgerConfig(maxQacctRetries = maxRuns - 1)
    
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")
    
    var timesQacctInvoked = 0
    
    val expectedRawData = actualQacctOutput(Option(expectedQueue), Option(expectedNode))
    
    def invokeQacct(jobId: String): Try[RunResults] = {
      timesQacctInvoked += 1
      
      if(timesQacctInvoked < 3) {
        Tries.failure("")
      } else {
        successfulRun(stdout = expectedRawData)
      }
    }
    
    val mockClient = new MockQacctAccountingClient(invokeQacct, ugerConfig, 0.001.seconds, 1.second)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    val expected = expectedResources(expectedRawData.mkString(newline), expectedNode, expectedQueue)
    
    assert(mockClient.getResourceUsage(jobId).get === expected)
    
    //Should have retried twice
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
    
    assert(mockClient.getResourceUsage(jobId).get === expected)
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetResourceUsageInvoked === 2)
  }

  test("getResourceUsage - no node to find") {
    val expectedQueue = Option(Queue("broad"))
    
    val expectedRawData = actualQacctOutput(expectedQueue, None)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val jobId = "12345"
    
    val expected = expectedResources(expectedRawData.mkString(newline), None, expectedQueue)
    
    assert(mockClient.getResourceUsage(jobId).get === expected)
    
    assert(expected.queue === expectedQueue)
    assert(expected.node === None)
  }

  test("getResourceUsage - no queue to find") {
    val expectedNode = Option("foo.example.com")
    
    val expectedRawData = actualQacctOutput(None, expectedNode)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val jobId = "12345"
    
    val expected = expectedResources(expectedRawData.mkString(newline), expectedNode, None)
    
    assert(mockClient.getResourceUsage(jobId).get === expected)
    
    assert(expected.queue === None)
    assert(expected.node === expectedNode)
  }

  test("getResourceUsage - neither queue nor node present") {
    val expectedRawData = actualQacctOutput(None, None)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val jobId = "12345"

    val expected = expectedResources(expectedRawData.mkString(newline), None, None)
    
    assert(mockClient.getResourceUsage(jobId).get === expected)
    
    assert(expected.queue === None)
    assert(expected.node === None)
  }

  test("getResourceUsage - junk output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq("foo", "bar", "baz")))

    val jobId = "12345"

    assert(mockClient.getResourceUsage(jobId).isFailure)
  }

  test("getQueue,getExecutionNode - empty output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq.empty))

    val jobId = "12345"

    assert(mockClient.getResourceUsage(jobId).isFailure)
  }
}
