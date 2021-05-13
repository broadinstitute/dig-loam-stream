package loamstream.apps

import java.nio.file.Paths

import org.scalatest.FunSuite
import org.scalatest.Matchers

import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.conf.LoamConfig
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.SlickLoamDao
import loamstream.drm.DrmChunkRunner
import loamstream.drm.DrmSystem
import loamstream.drm.PathBuilder
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.ByNameJobFilter
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.RunsIfNoOutputsJobFilter
import loamstream.model.execute.HashingStrategy
import loamstream.model.execute.RequiresPresentInputsJobCanceler
import loamstream.model.execute.ProtectsFilesJobCanceler
import loamstream.TestHelpers
import loamstream.util.Files

import scala.collection.compat._


/**
 * @author clint
 * Nov 14, 2016
 */
final class AppWiringTest extends FunSuite with Matchers {
  private val exampleFile = "src/examples/loam/cp.loam"
  
  private val exampleFilePath = Paths.get(exampleFile)
  
  private val confFileForUger = Paths.get("src/test/resources/for-uger-and-lsf.conf")
  
  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").to(Seq))
  
  //TODO: Purely expedient
  private def appWiring(cli: Conf): AppWiring = {
    val intent = Intent.from(cli).right.get.asInstanceOf[Intent.RealRun]
    
    AppWiring.forRealRun(intent, makeDao = _ => AppWiring.makeDaoFrom(DbDescriptor.inMemoryHsqldb))
  }
  
  test("Local execution, db-backed") {
    val wiring = appWiring(cliConf(s"--loams $exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val executer = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    executer.runner shouldBe a[CompositeChunkRunner]
    
    val compositeRunner = executer.runner.asInstanceOf[CompositeChunkRunner]
    
    compositeRunner.components.map(_.getClass) shouldBe(Seq(classOf[AsyncLocalChunkRunner]))
    
    import JobFilter.{AndJobFilter, OrJobFilter}
    
    val OrJobFilter(jf1, jf2: DbBackedJobFilter) = executer.jobFilter
    
    assert(jf1 === RunsIfNoOutputsJobFilter)
    assert(jf2.dao eq wiring.dao)
    assert(jf2.outputHashingStrategy === HashingStrategy.HashOutputs)
  }
  
  test("Local execution, run everything") {
    val wiring = appWiring(cliConf(s"--run everything --loams $exampleFile"))
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val executer = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    executer.runner shouldBe a[CompositeChunkRunner]
    
    val compositeRunner = executer.runner.asInstanceOf[CompositeChunkRunner]
    
    compositeRunner.components.map(_.getClass) shouldBe(Seq(classOf[AsyncLocalChunkRunner]))
    
    executer.jobFilter shouldBe JobFilter.RunEverything
  }
  
  private def toFlag(drmSystem: DrmSystem): String = drmSystem match {
    case DrmSystem.Uger => "--backend uger"
    case DrmSystem.Lsf => "--backend lsf"
  }
  
  test("DRM execution, db-backed") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val wiring = appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger --loams $exampleFile"))
      
      assert(wiring.dao.isInstanceOf[SlickLoamDao])
  
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.isInstanceOf[RxExecuter])
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      assert(actualExecuter.runner.isInstanceOf[CompositeChunkRunner])
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).to(Set) === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
      
      import JobFilter.{AndJobFilter, OrJobFilter}
      
      val OrJobFilter(jf1, jf2: DbBackedJobFilter) = actualExecuter.jobFilter
    
      assert(jf1 === RunsIfNoOutputsJobFilter)
      assert(jf2.dao eq wiring.dao)
      assert(jf2.outputHashingStrategy === HashingStrategy.HashOutputs)
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("DRM execution, filter jobs by name") {
    def doTest(drmSystem: DrmSystem, filterType: String): Unit = {
      val commandLine = s"${toFlag(drmSystem)} --conf $confFileForUger --run $filterType a b c --loams $exampleFile"
      
      val wiring = appWiring(cliConf(commandLine))
      
      val expectedJobFilterClass: Class[_] = filterType match {
        case Conf.RunStrategies.AllOf => classOf[ByNameJobFilter.AllOf]
        case Conf.RunStrategies.AnyOf => classOf[ByNameJobFilter.AnyOf]
        case Conf.RunStrategies.NoneOf => classOf[ByNameJobFilter.NoneOf]
        case _ => sys.error(s"Unexpected filter type '$filterType'")
      }
      
      assert(wiring.jobFilter.getClass === expectedJobFilterClass)
      
      assert(wiring.dao.isInstanceOf[SlickLoamDao])
  
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.isInstanceOf[RxExecuter])
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      assert(actualExecuter.runner.isInstanceOf[CompositeChunkRunner])
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).to(Set) === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
    }
    
    doTest(DrmSystem.Uger, Conf.RunStrategies.AllOf)
    doTest(DrmSystem.Uger, Conf.RunStrategies.AnyOf)
    doTest(DrmSystem.Uger, Conf.RunStrategies.NoneOf)
    doTest(DrmSystem.Lsf, Conf.RunStrategies.AllOf)
    doTest(DrmSystem.Lsf, Conf.RunStrategies.AnyOf)
    doTest(DrmSystem.Lsf, Conf.RunStrategies.NoneOf)
  }
  
  test("DRM execution, run everything") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val wiring = {
        appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger --run everything --loams $exampleFile"))
      }
      
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.isInstanceOf[RxExecuter])
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      assert(actualExecuter.runner.isInstanceOf[CompositeChunkRunner])
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).to(Set) === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
      
      actualExecuter.asInstanceOf[RxExecuter].jobFilter shouldBe JobFilter.RunEverything
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("DRM execution, correct PathBuilder is used by DrmChunkRunner") {
    def doTest(drmSystem: DrmSystem, expectedPathBuilder: PathBuilder): Unit = {
      val wiring = {
        appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger --loams $exampleFile"))
      }
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).to(Set) === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
      
      val drmRunner = runner.components.collectFirst { case drmRunner: DrmChunkRunner => drmRunner }.get
      
      assert(drmRunner.pathBuilder === expectedPathBuilder)
    }

    val loamConfig = LoamConfig.fromPath(confFileForUger).get
    
    doTest(DrmSystem.Uger, new UgerPathBuilder(UgerScriptBuilderParams(loamConfig.ugerConfig.get)))
    
    doTest(DrmSystem.Lsf, LsfPathBuilder)
  }
  
  test("loamConfigFrom") {
    import AppWiring.loamConfigFrom
    
    val expectedBaseConfig = LoamConfig.fromPath(confFileForUger).get
    
    def doTest(drmSystemOpt: Option[DrmSystem], shouldValidate: Boolean, cliConf: Option[Conf]): Unit = {
      
      val withDrmSystem = expectedBaseConfig.copy(drmSystem = drmSystemOpt)
      
      val newCompilationConfig = withDrmSystem.compilationConfig.copy(shouldValidateGraph = shouldValidate)
      
      val expected = withDrmSystem.copy(compilationConfig = newCompilationConfig).copy(cliConfig = cliConf)
      
      assert(loamConfigFrom(Some(confFileForUger), drmSystemOpt, shouldValidate, cliConf) === expected)
    }
    
    val conf = Conf("--loams src/example/scala/cp.scala".split("\\s+"))
    
    doTest(None, true, None)
    doTest(Some(DrmSystem.Uger), true, None)
    doTest(Some(DrmSystem.Lsf), true, None)
    doTest(None, true, Some(conf))
    doTest(Some(DrmSystem.Uger), true, Some(conf))
    doTest(Some(DrmSystem.Lsf), true, Some(conf))
    
    doTest(None, false, None)
    doTest(Some(DrmSystem.Uger), false, None)
    doTest(Some(DrmSystem.Lsf), false, None)
    doTest(None, false, Some(conf))
    doTest(Some(DrmSystem.Uger), false, Some(conf))
    doTest(Some(DrmSystem.Lsf), false, Some(conf))
  }
  
  test("defaultJobCanceler") {
    val expected0 = RequiresPresentInputsJobCanceler || ProtectsFilesJobCanceler.empty
    
    assert(AppWiring.defaultJobCanceller(None) === expected0)
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("foo.list")
      
      import java.nio.file.{ Paths => JPaths }
      import loamstream.util.{ Paths => LPaths }
      
      val rawNames = Seq("foo", "bar", "baz")
      
      val protectedFiles = rawNames.map(TestHelpers.path).map(LPaths.normalize)
      
      val protectsFilesCanceler = ProtectsFilesJobCanceler(protectedFiles)
      
      val expected1 = RequiresPresentInputsJobCanceler || protectsFilesCanceler
      
      Files.writeTo(file)(rawNames.mkString(System.lineSeparator))
      
      assert(AppWiring.defaultJobCanceller(Some(file)) === expected1)
    }
  }
}
