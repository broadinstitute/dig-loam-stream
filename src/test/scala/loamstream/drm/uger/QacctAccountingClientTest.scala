package loamstream.drm.uger

import org.scalatest.FunSuite

import loamstream.drm.Queue

/**
 * @author clint
 * Mar 15, 2017
 */
final class QacctAccountingClientTest extends FunSuite {

  import QacctTestHelpers.actualQacctOutput

  test("QacctUgerClient.getExecutionNode") {
    val expectedNode: String = "uger-c052.broadinstitute.org"
    val expectedQueue: Queue = Queue("broad")

    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))

    val jobId = "12345"
    val actualNode = mockClient.getExecutionNode(jobId)

    assert(actualNode === Some(expectedNode))

    assert(mockClient.timesGetExecutionNodeInvoked() === 1)
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 0)

    assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))

    assert(mockClient.timesGetExecutionNodeInvoked() === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 0)
  }

  test("QacctUgerClient.getQueue") {
    val expectedQueue: Queue = Queue("broad")
    val expectedNode: String = "foo.example.com"
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === Some(expectedQueue))

    assert(mockClient.timesGetExecutionNodeInvoked() === 0)
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 1)

    assert(mockClient.getQueue(jobId) === Some(expectedQueue))

    assert(mockClient.timesGetExecutionNodeInvoked() === 0)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 2)
  }

  test("QacctUgerClient.getExecutionNode - no node to find") {
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(Some(Queue("broad")), None))

    val jobId = "12345"

    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 1)
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 0)

    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 0)
  }

  test("QacctUgerClient.getQueue - no queue to find") {
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(None, Some("foo.example.com")))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 0)
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 1)

    assert(mockClient.getQueue(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 0)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 2)
  }

  test("QacctUgerClient.getQueue,getExecutionNode - neither present") {
    val mockClient = new MockQacctAccountingClient(_ => actualQacctOutput(None, None))

    val jobId = "12345"

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 1)
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 1)

    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)

    assert(mockClient.timesGetExecutionNodeInvoked() === 2)
    //Invocations of qacct should be memoized
    assert(mockClient.timesGetQacctOutputForInvoked() === 1)
    assert(mockClient.timesGetQueueInvoked() === 2)
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