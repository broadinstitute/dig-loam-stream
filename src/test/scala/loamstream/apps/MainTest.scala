package loamstream.apps

import org.scalatest.FunSuite

import java.nio.file.Files.exists
import java.nio.file.Path

import loamstream.conf.LoamConfig
import loamstream.db.LoamDao
import loamstream.model.execute.Executer
import loamstream.googlecloud.CloudStorageClient
import loamstream.model.execute.JobFilter
import loamstream.model.execute.ExecutionRecorder
import loamstream.cli.Intent
import loamstream.db.slick.DbDescriptor
import loamstream.conf.Locations
import loamstream.TestHelpers
import loamstream.drm.DrmSystem
import loamstream.conf.UgerConfig
import loamstream.conf.LsfConfig
import loamstream.conf.ExecutionConfig
import loamstream.conf.CompilationConfig



/**
 * @author clint
 * Mar 31, 2017
 */
final class MainTest extends FunSuite {
  test("shutdown") {
    val mockWiring = new MainTest.MockAppWiring
    
    val run = new Main.Run
    
    assert(mockWiring.shutdownInvocations === 0)
    
    run.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    run.shutdown(mockWiring)
    
    assert(mockWiring.shutdownInvocations === 1)
    
    run.shutdown(mockWiring)
    
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

    override def jobFilter: JobFilter = ???
    
    override def executionRecorder: ExecutionRecorder = ???
    
    override def writeIndexFiles(): Unit = ???
    
    override def shutdown(): Seq[Throwable] = {
      shutdownInvocations += 1
      
      Nil
    }
  }
}
