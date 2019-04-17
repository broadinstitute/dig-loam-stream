package loamstream.drm.uger

import org.scalatest.FunSuite

import loamstream.drm.Queue
import loamstream.conf.UgerConfig

/**
 * @author clint
 * Mar 15, 2017
 */
final class QacctAccountingClientTest extends FunSuite {

  import QacctTestHelpers.actualQacctOutput
  import scala.concurrent.duration._

  test("delaySequence - defaults") {
    val delays = QacctAccountingClient.delaySequence(
        QacctAccountingClient.defaultDelayStart, 
        QacctAccountingClient.defaultDelayCap)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.5.seconds, 
        1.second, 
        2.seconds, 
        4.seconds, 
        8.seconds, 
        16.seconds,
        30.seconds,
        30.seconds,
        30.seconds,
        30.seconds)
        
    assert(actual === expected)
  }
  
  test("delaySequence - non-defaults, cap not hit") {
    val delays = QacctAccountingClient.delaySequence(0.01.seconds, 10.seconds)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.01.seconds, 
        0.02.second, 
        0.04.seconds, 
        0.08.seconds, 
        0.16.seconds, 
        0.32.seconds,
        0.64.seconds,
        1.28.seconds,
        2.56.seconds,
        5.12.seconds)
        
    assert(actual === expected)
  }
  
  test("delaySequence - non-defaults, cap hit") {
    val delays = QacctAccountingClient.delaySequence(0.01.seconds, 1.seconds)
    
    val actual = delays.take(10).toIndexedSeq
    
    val expected = Seq(
        0.01.seconds, 
        0.02.second, 
        0.04.seconds, 
        0.08.seconds, 
        0.16.seconds, 
        0.32.seconds,
        0.64.seconds,
        1.second,
        1.second,
        1.second)
        
    assert(actual === expected)
  }
  
  test("retries - never works") {
    val maxRuns = 6
    
    val ugerConfig = UgerConfig(maxQacctRetries = maxRuns - 1)
    
    val mockClient = new MockQacctAccountingClient(_ => throw new Exception, ugerConfig, 0.001.seconds, 0.5.seconds)
    
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
    
    def invokeQacct(jobId: String): Seq[String] = {
      timesQacctInvoked += 1
      
      if(timesQacctInvoked < 3) {
        throw new Exception
      } else {
        actualQacctOutput(Some(expectedQueue), Some(expectedNode))
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

    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))

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
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))

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
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(Queue("broad")), None))

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
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(None, Some("foo.example.com")))

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
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(None, None))

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
    val mockClient = new MockQacctAccountingClient(_ => Seq("foo", "bar", "baz"))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }

  test("QacctUgerClient.getQueue,getExecutionNode - empty output") {
    val mockClient = new MockQacctAccountingClient(_ => Seq.empty)

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }

  test("makeTokens") {
    assert(QacctAccountingClient.makeTokens("foo", "bar") === Seq("foo", "-j", "bar"))
  }
}
