package loamstream.apps

import org.scalatest.FunSuite
import loamstream.conf.LoamConfig
import loamstream.db.LoamDao
import loamstream.model.execute.Executer
import loamstream.googlecloud.CloudStorageClient
import loamstream.model.execute.JobFilter
import loamstream.model.execute.ExecutionRecorder
import loamstream.cli.Intent
import loamstream.db.slick.DbDescriptor
import loamstream.conf.Locations
import java.nio.file.Path

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
  
  test("doClean - clean db") {
    import java.nio.file.Files.exists
    
    def makeIfNecessary(p: Path): Unit = {
      p.toFile.mkdirs()
      
      assert(exists(p))
    }
    
    def doTest(intent: Intent.Clean): Unit = {
      val run = new Main.Run
      
      val dbDirShouldExistAfterClean = !intent.db
      val logDirsShouldExistAfterClean = !intent.logs
      val scriptDirsShouldExistAfterClean = !intent.scripts
      
      makeIfNecessary(DbDescriptor.defaultDbDir)
      
      makeIfNecessary(Locations.logDir)
      makeIfNecessary(Locations.jobOutputDir)
      
      makeIfNecessary(Locations.ugerDir)
      makeIfNecessary(Locations.lsfDir)
      
      run.doClean(intent)
      
      assert(exists(DbDescriptor.defaultDbDir) === dbDirShouldExistAfterClean)
      
      assert(exists(Locations.logDir) === logDirsShouldExistAfterClean)
      assert(exists(Locations.jobOutputDir) === logDirsShouldExistAfterClean)
      
      assert(exists(Locations.ugerDir) === scriptDirsShouldExistAfterClean)
      assert(exists(Locations.lsfDir) === scriptDirsShouldExistAfterClean)
    }
    
    doTest(Intent.Clean(None, db = true, logs = false, scripts = false))
    doTest(Intent.Clean(None, db = false, logs = true, scripts = false))
    doTest(Intent.Clean(None, db = false, logs = false, scripts = true))
    doTest(Intent.Clean(None, db = true, logs = true, scripts = true))
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
    
    override def shutdown(): Seq[Throwable] = {
      shutdownInvocations += 1
      
      Nil
    }
  }
}
