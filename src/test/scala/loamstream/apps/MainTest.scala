package loamstream.apps

import org.scalatest.FunSuite
import loamstream.conf.LoamConfig
import loamstream.db.LoamDao
import loamstream.model.execute.Executer
import loamstream.googlecloud.CloudStorageClient

/**
 * @author clint
 * Mar 31, 2017
 */
final class MainTest extends FunSuite {
  test("shutdown") {
    val mockWiring = new MainTest.MockAppWiring
    
    assert(mockWiring.shutdownInvocations === 0)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    Main.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
  }
}

object MainTest {
  final class MockAppWiring extends AppWiring {
    var shutdownInvocations: Int = 0
    
    override def config: LoamConfig = ???
  
    override def dao: LoamDao = ???

    override def executer: Executer = ???

    override def cloudStorageClient: Option[CloudStorageClient] = ???

    override def shutdown(): Seq[Throwable] = {
      shutdownInvocations += 1
      
      Nil
    }
  }
}
