package loamstream.drm.uger

import java.time.LocalDateTime

import scala.util.Try

import org.scalatest.FunSuite

import loamstream.conf.UgerConfig
import loamstream.drm.Queue
import loamstream.util.RunResults
import loamstream.util.Tries
import java.time.Month
import loamstream.drm.DrmTaskId

/**
 * @author clint
 * Mar 15, 2017
 */
final class QacctAccountingClientTest extends FunSuite {

  import QacctTestHelpers.actualQacctOutput
  import QacctTestHelpers.expectedResources
  import QacctTestHelpers.successfulRun
  import loamstream.TestHelpers.waitFor
  import scala.concurrent.duration._

  private val newline: String = scala.util.Properties.lineSeparator
  
  private def makeStartAndEndTime: (LocalDateTime, LocalDateTime) = {
    val start = LocalDateTime.now
    
    val end = start.plusSeconds(42)
    
    (start, end)
  }
  
  test("toLocalDateTime - round trip") {
    val now = LocalDateTime.now
    
    val inUgerFormat = QacctTestHelpers.toUgerFormat(now)
    
    val parsed = QacctAccountingClient.toLocalDateTime("foo")(inUgerFormat)
    
    assert(parsed.get === now)
  }
  
  test("toLocalDateTime - problematic dates") {
    def doTest(ugerFormat: String, expected: LocalDateTime): Unit = {
      val parsed = QacctAccountingClient.toLocalDateTime("foo")(ugerFormat)
      
      assert(parsed.get === expected)
    }
    
    doTest("04/25/2019 14:20:36.264", LocalDateTime.of(2019, Month.APRIL, 25, 14, 20, 36, 264 * 1000 * 1000))
    doTest("03/06/2017 17:49:50.505", LocalDateTime.of(2017, Month.MARCH, 6, 17, 49, 50, 505 * 1000 *1000))
  }
  
  test("getResourceUsage - accounting client fails") {
    val ugerConfig = UgerConfig(maxRetries = 0)
    
    val mockClient = new MockQacctAccountingClient(_ => Tries.failure("blarg"), ugerConfig)
    
    val taskId = DrmTaskId("abc123", 42)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    val result = waitFor(mockClient.getResourceUsage(taskId).failed)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
  }
  
  test("getResourceUsage - happy path") {
    val ugerConfig = UgerConfig()
    
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")

    val (startTime, endTime) = makeStartAndEndTime
    
    val qacctOutput = actualQacctOutput(Some(expectedQueue), Some(expectedNode), startTime, endTime)
    
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = qacctOutput), ugerConfig)
    
    val taskId = DrmTaskId("abc123", 42)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    val actualResources = waitFor(mockClient.getResourceUsage(taskId))
    
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetResourceUsageInvoked === 1)

    val expectedRawData = {
      actualQacctOutput(Option(expectedQueue), Option(expectedNode), startTime, endTime).mkString(newline)
    }
    
    assert(actualResources === expectedResources(expectedRawData, expectedNode, expectedQueue, startTime, endTime))
  }
  
  test("retries - never works") {
    val maxRuns = 6
    
    val ugerConfig = UgerConfig(maxRetries = maxRuns - 1)
    
    val mockClient = new MockQacctAccountingClient(_ => Tries.failure("blarg"), ugerConfig, 0.001.seconds, 0.5.seconds)
    
    val taskId = DrmTaskId("abc123", 42)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    waitFor(mockClient.getResourceUsage(taskId).failed)
    
    //Should have retried 
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
    
    waitFor(mockClient.getResourceUsage(taskId).failed)
    
    //should have memoized results, and not retried any more
    //assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    //assert(mockClient.timesGetResourceUsageInvoked === 2)
  }
  
  test("retries - works after 2 failures") {
    val maxRuns = 5
    
    val ugerConfig = UgerConfig(maxRetries = maxRuns - 1)
    
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")
    
    var timesQacctInvoked = 0
    
    val (startTime, endTime) = makeStartAndEndTime
    
    val expectedRawData = actualQacctOutput(Option(expectedQueue), Option(expectedNode), startTime, endTime)
    
    def invokeQacct(taskId: DrmTaskId): Try[RunResults] = {
      timesQacctInvoked += 1
      
      if(timesQacctInvoked < 3) {
        Tries.failure("")
      } else {
        successfulRun(stdout = expectedRawData)
      }
    }
    
    val mockClient = new MockQacctAccountingClient(invokeQacct, ugerConfig, 0.001.seconds, 1.second)
    
    val taskId = DrmTaskId("abc123", 42)
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetResourceUsageInvoked === 0)
    
    val expected = {
      expectedResources(expectedRawData.mkString(newline), expectedNode, expectedQueue, startTime, endTime)
    }
    
    assert(waitFor(mockClient.getResourceUsage(taskId))=== expected)
    
    //Should have retried twice
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetResourceUsageInvoked === 1)
    
    assert(waitFor(mockClient.getResourceUsage(taskId)) === expected)
    
    //should have memoized results, and not retried any more
    //assert(mockClient.timesGetQacctOutputForInvoked === 3)
    //assert(mockClient.timesGetResourceUsageInvoked === 2)
  }

  test("getResourceUsage - no node to find") {
    val expectedQueue = Option(Queue("broad"))
    
    val (startTime, endTime) = makeStartAndEndTime
    
    val expectedRawData = actualQacctOutput(expectedQueue, None, startTime, endTime)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val taskId = DrmTaskId("12345", 42)
    
    val expected = expectedResources(expectedRawData.mkString(newline), None, expectedQueue, startTime, endTime)
    
    assert(waitFor(mockClient.getResourceUsage(taskId)) === expected)
    
    assert(expected.queue === expectedQueue)
    assert(expected.node === None)
  }

  test("getResourceUsage - no queue to find") {
    val expectedNode = Option("foo.example.com")
    
    val (startTime, endTime) = makeStartAndEndTime
    
    val expectedRawData = actualQacctOutput(None, expectedNode, startTime, endTime)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val taskId = DrmTaskId("12345", 42)
    
    val expected = expectedResources(expectedRawData.mkString(newline), expectedNode, None, startTime, endTime)
    
    assert(waitFor(mockClient.getResourceUsage(taskId)) === expected)
    
    assert(expected.queue === None)
    assert(expected.node === expectedNode)
  }

  test("getResourceUsage - neither queue nor node present") {
    val (startTime, endTime) = makeStartAndEndTime
    
    val expectedRawData = actualQacctOutput(None, None, startTime, endTime)
    
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = expectedRawData)
    )

    val taskId = DrmTaskId("12345", 42)

    val expected = expectedResources(expectedRawData.mkString(newline), None, None, startTime, endTime)
    
    assert(waitFor(mockClient.getResourceUsage(taskId)) === expected)
    
    assert(expected.queue === None)
    assert(expected.node === None)
  }

  test("getResourceUsage - junk output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq("foo", "bar", "baz")))

    val taskId = DrmTaskId("12345", 42)

    waitFor(mockClient.getResourceUsage(taskId).failed)
  }

  test("getQueue,getExecutionNode - empty output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq.empty))

    val taskId = DrmTaskId("12345", 42)

    waitFor(mockClient.getResourceUsage(taskId).failed)
  }
}
