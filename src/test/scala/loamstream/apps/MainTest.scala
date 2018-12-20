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
  
  private def makeIfNecessary(ps: Path*): Unit = ps.foreach { p =>
    p.toFile.mkdirs()
    
    assert(exists(p))
  }
  
  private def makeIfNecessary(pos: Option[Path]*)(implicit discriminator: Int = 42): Unit = {
    pos.flatten.foreach(makeIfNecessary(_))
  }
  
  test("doClean - clean db") {

    def getUgerConfig(drmSystem: DrmSystem, workDir: Path): Option[UgerConfig] = drmSystem match {
      case DrmSystem.Uger => {
        Some(UgerConfig(workDir = workDir.resolve("work"), scriptDir = workDir.resolve("scripts")))
      }
      case _ => None
    }
    
    def getLsfConfig(drmSystem: DrmSystem, workDir: Path): Option[LsfConfig] = drmSystem match {
      case DrmSystem.Lsf => {
        Some(LsfConfig(workDir = workDir.resolve("work"), scriptDir = workDir.resolve("scripts")))
      }
      case _ => None
    }
    
    def getPaths: (Path, Path, Path, Path, Path) = {
      val workDir = TestHelpers.getWorkDir(getClass.getSimpleName)
      
      val dbDir = workDir.resolve("db")
      val logDir = workDir.resolve("logs")
      val jobOutputDir = workDir.resolve("job-outputs")
      
      val dryRunOutputFile = workDir.resolve("joblist")
      
      (workDir, dbDir, logDir, jobOutputDir, dryRunOutputFile)
    }
    
    def doTest(intent: Intent.Clean, drmSystem: DrmSystem): Unit = {
      val run = new Main.Run
      
      val (workDir, dbDir, logDir, jobOutputDir, dryRunOutputFile) = getPaths
      
      val executionConfig = ExecutionConfig(
          jobOutputDir = jobOutputDir, 
          dryRunOutputFile = dryRunOutputFile,
          dbDir = dbDir,
          logDir = logDir)
          
      val ugerConfigOpt = getUgerConfig(drmSystem, workDir)
      val lsfConfigOpt = getLsfConfig(drmSystem, workDir)
      
      val config = LoamConfig(
          ugerConfig = ugerConfigOpt,
          lsfConfig = lsfConfigOpt, 
          googleConfig = None,
          hailConfig = None, 
          pythonConfig = None, 
          rConfig = None, 
          executionConfig = executionConfig, 
          compilationConfig = CompilationConfig.default, 
          drmSystem = Option(drmSystem))
      
      val dbDirShouldExistAfterClean = !intent.db
      val logDirsShouldExistAfterClean = !intent.logs
      val scriptDirsShouldExistAfterClean = !intent.scripts
      
      makeIfNecessary(dbDir, logDir, jobOutputDir)
      makeIfNecessary(ugerConfigOpt.map(_.workDir), lsfConfigOpt.map(_.workDir))
      makeIfNecessary(ugerConfigOpt.map(_.scriptDir), lsfConfigOpt.map(_.scriptDir))
      
      run.actuallyDoClean(intent, config)
      
      assert(exists(dbDir) === dbDirShouldExistAfterClean)
      assert(exists(logDir) === logDirsShouldExistAfterClean)
      assert(exists(jobOutputDir) === logDirsShouldExistAfterClean)
      
      for(ugerConfig <- ugerConfigOpt) {
        assert(exists(ugerConfig.workDir) === scriptDirsShouldExistAfterClean)
        assert(exists(ugerConfig.scriptDir) === scriptDirsShouldExistAfterClean)
      }
      
      for(lsfConfig <- lsfConfigOpt) {
        assert(exists(lsfConfig.workDir) === scriptDirsShouldExistAfterClean)
        assert(exists(lsfConfig.scriptDir) === scriptDirsShouldExistAfterClean)
      }
    }
    
    doTest(Intent.Clean(None, db = true, logs = false, scripts = false), DrmSystem.Uger)
    doTest(Intent.Clean(None, db = false, logs = true, scripts = false), DrmSystem.Uger)
    doTest(Intent.Clean(None, db = false, logs = false, scripts = true), DrmSystem.Uger)
    doTest(Intent.Clean(None, db = true, logs = true, scripts = true), DrmSystem.Uger)
    
    doTest(Intent.Clean(None, db = true, logs = false, scripts = false), DrmSystem.Lsf)
    doTest(Intent.Clean(None, db = false, logs = true, scripts = false), DrmSystem.Lsf)
    doTest(Intent.Clean(None, db = false, logs = false, scripts = true), DrmSystem.Lsf)
    doTest(Intent.Clean(None, db = true, logs = true, scripts = true), DrmSystem.Lsf)
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
