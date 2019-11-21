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
import loamstream.conf.ExecutionConfig
import loamstream.cli.JobFilterIntent



/**
 * @author clint
 * Nov 14, 2016
 */
final class AppWiringTest extends FunSuite with Matchers {
  private val exampleFile = "src/examples/loam/cp.loam"
  
  private val exampleFilePath = Paths.get(exampleFile)
  
  private val confFileForUger = Paths.get("src/test/resources/for-uger-and-lsf.conf")
  
  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").toSeq)
  
  //TODO: Purely expedient
  private def appWiring(cli: Conf): AppWiring = {
    val intent = Intent.from(cli).right.get.asInstanceOf[Intent.RealRun]
    
    AppWiring.forRealRun(intent, makeDao = AppWiring.makeDaoFrom(DbDescriptor.inMemoryHsqldb))
  }
  
  test("executerWindowLength") {
    import AppWiring.executerWindowLength
    import scala.concurrent.duration._
    
    val config = ExecutionConfig.default.copy(windowLength = 42.seconds)
    val runByNameIntent = Intent.RealRun(
      confFile = None,
      shouldValidate = false,
      hashingStrategy = HashingStrategy.DontHashOutputs,
      jobFilterIntent = JobFilterIntent.RunIfAllMatch(Nil),
      drmSystemOpt = None,
      loams = Nil)
      
    assert(executerWindowLength(config, runByNameIntent) === 1.millisecond)
    
    val dontRunByNameIntent = runByNameIntent.copy(jobFilterIntent = JobFilterIntent.DontFilterByName)
    
    assert(executerWindowLength(config, dontRunByNameIntent) === 42.seconds)
    
    val runEverythingIntent = runByNameIntent.copy(jobFilterIntent = JobFilterIntent.RunEverything)
    
    assert(executerWindowLength(config, runEverythingIntent) === 42.seconds)
  }
  
  test("Local execution, db-backed") {
    val wiring = appWiring(cliConf(s"--loams $exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[RxExecuter]
    
    val executer = wiring.executer.asInstanceOf[RxExecuter]
    
    import JobFilter.{AndJobFilter, OrJobFilter}
    
    val OrJobFilter(jf1, jf2: DbBackedJobFilter) = executer.jobFilter
    
    assert(jf1 === RunsIfNoOutputsJobFilter)
    assert(jf2.dao eq wiring.dao)
    assert(jf2.outputHashingStrategy === HashingStrategy.HashOutputs)
    
    fail("TODO")
  }
  
  test("Local execution, run everything") {
    val wiring = appWiring(cliConf(s"--run everything --loams $exampleFile"))
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    val executer = wiring.executer.asInstanceOf[RxExecuter]
    
    executer.jobFilter shouldBe JobFilter.RunEverything
    
    fail("TODO")
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
      
      val actualExecuter = wiring.executer.asInstanceOf[RxExecuter]
      
      import JobFilter.{AndJobFilter, OrJobFilter}
      
      val OrJobFilter(jf1, jf2: DbBackedJobFilter) = actualExecuter.jobFilter
    
      assert(jf1 === RunsIfNoOutputsJobFilter)
      assert(jf2.dao eq wiring.dao)
      assert(jf2.outputHashingStrategy === HashingStrategy.HashOutputs)
      
      fail("TODO")
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
      
      val actualExecuter = wiring.executer.asInstanceOf[RxExecuter]
      
      fail("TODO")
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
      
      val actualExecuter = wiring.executer.asInstanceOf[RxExecuter]
      
      actualExecuter.jobFilter shouldBe JobFilter.RunEverything
      
      fail("TODO")
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("DRM execution, correct PathBuilder is used by DrmChunkRunner") {
    def doTest(drmSystem: DrmSystem, expectedPathBuilder: PathBuilder): Unit = {
      val wiring = {
        appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger --loams $exampleFile"))
      }
      
      fail("TODO")
    }

    val loamConfig = LoamConfig.fromPath(confFileForUger).get
    
    doTest(DrmSystem.Uger, new UgerPathBuilder(UgerScriptBuilderParams(loamConfig.ugerConfig.get)))
    
    doTest(DrmSystem.Lsf, LsfPathBuilder)
  }
  
  test("loamConfigFrom") {
    import AppWiring.loamConfigFrom
    
    val expectedBaseConfig = LoamConfig.fromPath(confFileForUger).get
    
    def doTest(drmSystemOpt: Option[DrmSystem], shouldValidate: Boolean): Unit = {
      
      val withDrmSystem = expectedBaseConfig.copy(drmSystem = drmSystemOpt)
      
      val newCompilationConfig = withDrmSystem.compilationConfig.copy(shouldValidateGraph = shouldValidate)
      
      val expected = withDrmSystem.copy(compilationConfig = newCompilationConfig)
      
      assert(loamConfigFrom(Some(confFileForUger), drmSystemOpt, shouldValidate) === expected)
    }
    
    doTest(None, true)
    doTest(Some(DrmSystem.Uger), true)
    doTest(Some(DrmSystem.Lsf), true)
    
    doTest(None, false)
    doTest(Some(DrmSystem.Uger), false)
    doTest(Some(DrmSystem.Lsf), false)
  }
  
  test("defaultJobCanceler") {
    assert(AppWiring.defaultJobCanceller === RequiresPresentInputsJobCanceler)
  }
}
