package loamstream.drm.uger

import org.scalatest.FunSuite

import loamstream.drm.Queue
import loamstream.conf.UgerConfig
import scala.util.Try
import loamstream.util.RunResults
import loamstream.util.Tries

/**
 * @author clint
 * Mar 15, 2017
 */
final class QacctAccountingClientTest extends FunSuite {

  import QacctTestHelpers.actualQacctOutput
  import QacctTestHelpers.successfulRun
  import scala.concurrent.duration._
  
  test("retries - never works") {
    val maxRuns = 6
    
    val ugerConfig = UgerConfig(maxQacctRetries = maxRuns - 1)
    
    val mockClient = new MockQacctAccountingClient(_ => Tries.failure("blarg"), ugerConfig, 0.001.seconds, 0.5.seconds)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getExecutionNode(jobId).isEmpty)
    
    //Should have retried once
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetExecutionNodeInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getExecutionNode(jobId).isEmpty)
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getQueue(jobId).isEmpty)
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === maxRuns)
    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    assert(mockClient.timesGetQueueInvoked === 1)
  }
  
  test("retries - works after 2 failures") {
    val maxRuns = 5
    
    val ugerConfig = UgerConfig(maxQacctRetries = maxRuns - 1)
    
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")
    
    var timesQacctInvoked = 0
    
    def invokeQacct(jobId: String): Try[RunResults] = {
      timesQacctInvoked += 1
      
      if(timesQacctInvoked < 3) {
        Tries.failure("")
      } else {
        successfulRun(stdout = actualQacctOutput(Some(expectedQueue), Some(expectedNode)))
      }
    }
    
    val mockClient = new MockQacctAccountingClient(invokeQacct, ugerConfig, 0.001.seconds, 1.second)
    
    val jobId = "abc123"
    
    assert(mockClient.timesGetQacctOutputForInvoked === 0)
    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))
    
    //Should have retried twice
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetExecutionNodeInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    assert(mockClient.timesGetQueueInvoked === 0)
    
    assert(mockClient.getQueue(jobId) === Some(expectedQueue))
    
    //should have memoized results, and not retried any more
    assert(mockClient.timesGetQacctOutputForInvoked === 3)
    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    assert(mockClient.timesGetQueueInvoked === 1)
  }
  
  test("QacctUgerClient.getExecutionNode") {
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")

    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = actualQacctOutput(Some(expectedQueue), Some(expectedNode)))
    )

    val jobId = "12345"
    val actualNode = mockClient.getExecutionNode(jobId)

    assert(actualNode === Some(expectedNode))

    assert(mockClient.timesGetExecutionNodeInvoked === 1)
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)

    assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))

    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)
  }

  test("QacctUgerClient.getQueue") {
    val expectedQueue: Queue = Queue("broad")
    val expectedNode: String = "foo.example.com"
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = actualQacctOutput(Some(expectedQueue), Some(expectedNode)))
    )

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === Some(expectedQueue))

    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 1)

    assert(mockClient.getQueue(jobId) === Some(expectedQueue))

    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 2)
  }

  test("QacctUgerClient.getExecutionNode - no node to find") {
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = actualQacctOutput(Some(Queue("broad")), None))
    )

    val jobId = "12345"

    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 1)
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)

    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 0)
  }

  test("QacctUgerClient.getQueue - no queue to find") {
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = actualQacctOutput(None, Some("foo.example.com")))
    )

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 1)

    assert(mockClient.getQueue(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 0)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 2)
  }

  test("QacctUgerClient.getQueue,getExecutionNode - neither present") {
    val mockClient = new MockQacctAccountingClient(_ => 
      successfulRun(stdout = actualQacctOutput(None, None))
    )

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 1)
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 1)

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked === 1)
    assert(mockClient.timesGetQueueInvoked === 2)
  }

  test("QacctUgerClient.getQueue,getExecutionNode - junk output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq("foo", "bar", "baz")))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }

  test("QacctUgerClient.getQueue,getExecutionNode - empty output") {
    val mockClient = new MockQacctAccountingClient(_ => successfulRun(stdout = Seq.empty))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }

  test("makeTokens") {
    assert(QacctAccountingClient.makeTokens("foo", "bar") === Seq("foo", "-j", "bar"))
  }
}
