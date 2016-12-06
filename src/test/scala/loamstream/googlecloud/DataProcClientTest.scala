package loamstream.googlecloud

import loamstream.util.ValueBox
import org.scalatest.FunSuite
import loamstream.googlecloud.DataProcClientTest.MockDataProcClient
import scala.concurrent.Future
import loamstream.util.Futures

/**
 * @author clint
 * Nov 29, 2016
 */
final class DataProcClientTest extends FunSuite {
  import DataProcClientTest.MockDataProcClient
  
  //scalastyle:off magic.number
  
  test("doWithCluster") {
    val client = new MockDataProcClient
    
    import scala.concurrent.ExecutionContext.Implicits.global
    
    def doDoWithCluster[A](a: => A): A = {
      Futures.waitFor(client.doWithCluster(Future(a)))
    }
    
    assert(client.startClusterInvocations() === 0)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 0)
    assert(client.clusterRunning() === false)
    
    val x = doDoWithCluster {
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 0)
      assert(client.isClusterRunningInvocations() === 0)
      assert(client.clusterRunning() === true)
      
      42
    }
    
    assert(x === 42)
    
    assert(client.startClusterInvocations() === 1)
    assert(client.deleteClusterInvocations() === 1)
    assert(client.isClusterRunningInvocations() === 1)
    assert(client.clusterRunning() === false)
    
    client.reset()
    
    assert(client.startClusterInvocations() === 0)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 0)
    assert(client.clusterRunning() === false)
    
    intercept[Exception] {
      doDoWithCluster {
        throw new Exception
      }
      
      assert(client.startClusterInvocations() === 1)
      assert(client.deleteClusterInvocations() === 1)
      assert(client.isClusterRunningInvocations() === 1)
      assert(client.clusterRunning() === false)
    }
  }
  
  test("deleteClusterIfRunning") {
    val client = new MockDataProcClient
    
    assert(client.startClusterInvocations() === 0)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 0)
    assert(client.clusterRunning() === false)
    
    client.deleteClusterIfRunning()
    
    assert(client.startClusterInvocations() === 0)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 1)
    assert(client.clusterRunning() === false)
    
    client.deleteClusterIfRunning()
    
    assert(client.startClusterInvocations() === 0)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 2)
    assert(client.clusterRunning() === false)
    
    client.startCluster()
    
    assert(client.startClusterInvocations() === 1)
    assert(client.deleteClusterInvocations() === 0)
    assert(client.isClusterRunningInvocations() === 2)
    assert(client.clusterRunning() === true)
    
    client.deleteClusterIfRunning()
    
    assert(client.startClusterInvocations() === 1)
    assert(client.deleteClusterInvocations() === 1)
    assert(client.isClusterRunningInvocations() === 3)
    assert(client.clusterRunning() === false)
  }
  //scalastyle:on magic.number
}

object DataProcClientTest {
  //scalastyle:off magic.number
  final class MockDataProcClient extends DataProcClient {
    val startClusterInvocations: ValueBox[Int] = ValueBox(0)
    val deleteClusterInvocations: ValueBox[Int] = ValueBox(0)
    val isClusterRunningInvocations: ValueBox[Int] = ValueBox(0)
    
    val clusterRunning: ValueBox[Boolean] = ValueBox(false)
    
    def reset(): Unit = {
      startClusterInvocations.update(0)
      deleteClusterInvocations.update(0)
      isClusterRunningInvocations.update(0)
      clusterRunning.update(false)
    }
    
    override def deleteCluster(): Unit = {
      deleteClusterInvocations.mutate(_ + 1)
      
      clusterRunning.update(false)
    }
  
    override def isClusterRunning: Boolean = {
      isClusterRunningInvocations.mutate(_ + 1)
      
      clusterRunning()
    }
    
    override def startCluster(): Unit = {
      startClusterInvocations.mutate(_ + 1)
      
      clusterRunning.update(true)
    }
  }
  //scalastyle:on magic.number
}