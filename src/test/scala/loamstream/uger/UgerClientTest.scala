package loamstream.uger

import org.scalatest.FunSuite
import loamstream.util.ValueBox

/**
 * @author clint
 * Mar 15, 2017
 */
final class UgerClientTest extends FunSuite {
  
  import QacctTestHelpers.actualQacctOutput
  
  test("QacctUgerClient.getExecutionNode") {
    def doTest(expectedQueue: Queue, expectedNode: String): Unit = {
      val mockClient = new MockUgerClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))
    
      val jobId = "12345"
    
      assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))
      
      assert(mockClient.timesGetExecutionNodeInvoked() === 1)
      assert(mockClient.timesGetQacctOutputForInvoked() === 1)
      assert(mockClient.timesGetQueueInvoked() === 0)
      
      assert(mockClient.getExecutionNode(jobId) === Some(expectedNode))
      
      assert(mockClient.timesGetExecutionNodeInvoked() === 2)
      //Invocations of qacct should be memoized
      assert(mockClient.timesGetQacctOutputForInvoked() === 1)
      assert(mockClient.timesGetQueueInvoked() === 0)
    }
    
    doTest(Queue.Short, "uger-c052.broadinstitute.org")
    doTest(Queue.Long, "uger-c052.broadinstitute.org")
  }
  
  test("QacctUgerClient.getQueue") {
    def doTest(expectedQueue: Queue, expectedNode: String): Unit = {
      val mockClient = new MockUgerClient(_ => actualQacctOutput(Some(expectedQueue), Some(expectedNode)))
      
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
    
    doTest(Queue.Short, "foo.example.com")
    doTest(Queue.Long, "foo.example.com")
  }
  
  test("QacctUgerClient.getExecutionNode - no node to find") {
    val mockClient = new MockUgerClient(_ => actualQacctOutput(Some(Queue.Short), None))
  
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
    val mockClient = new MockUgerClient(_ => actualQacctOutput(None, Some("foo.example.com")))
    
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
    val mockClient = new MockUgerClient(_ => actualQacctOutput(None, None))
    
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
    val mockClient = new MockUgerClient(_ => Seq("foo", "bar", "baz"))
    
    val jobId = "12345"
    
    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }
  
  test("QacctUgerClient.getQueue,getExecutionNode - empty output") {
    val mockClient = new MockUgerClient(_ => Seq.empty)
    
    val jobId = "12345"
    
    assert(mockClient.getQueue(jobId) === None)
    assert(mockClient.getExecutionNode(jobId) === None)
  }
  
  test("makeTokens") {
    assert(UgerClient.makeTokens("foo", "bar") === Seq("foo", "-j", "bar"))
  }
}
