package loamstream.apps

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.cli.Conf
import org.scalatest.Matchers
import loamstream.db.slick.SlickLoamDao
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.HashingStrategy
import loamstream.cli.Intent
import loamstream.TestHelpers
import loamstream.db.slick.DbDescriptor
import loamstream.drm.DrmSystem
import loamstream.drm.DrmChunkRunner


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
    
    AppWiring.forRealRun(intent, makeDao = AppWiring.makeDaoFrom(DbDescriptor.inMemory))
  }
  
  test("Local execution, db-backed") {
    val wiring = appWiring(cliConf(s"$exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val executer = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    executer.runner shouldBe a[CompositeChunkRunner]
    
    val compositeRunner = executer.runner.asInstanceOf[CompositeChunkRunner]
    
    compositeRunner.components.map(_.getClass) shouldBe(Seq(classOf[AsyncLocalChunkRunner]))
    
    executer.jobFilter shouldBe a[DbBackedJobFilter]
    
    executer.jobFilter.asInstanceOf[DbBackedJobFilter].dao shouldBe wiring.dao
  }
  
  test("Local execution, run everything") {
    val wiring = appWiring(cliConf(s"--run-everything $exampleFile"))
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val executer = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    executer.runner shouldBe a[CompositeChunkRunner]
    
    val compositeRunner = executer.runner.asInstanceOf[CompositeChunkRunner]
    
    compositeRunner.components.map(_.getClass) shouldBe(Seq(classOf[AsyncLocalChunkRunner]))
    
    executer.jobFilter shouldBe JobFilter.RunEverything
  }
  
  private def toFlag(drmSystem: DrmSystem): String = drmSystem match {
    case DrmSystem.Uger => "--uger"
    case DrmSystem.Lsf => "--lsf"
  }
  
  test("DRM execution, db-backed") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val wiring = appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger $exampleFile"))
      
      assert(wiring.dao.isInstanceOf[SlickLoamDao])
  
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.isInstanceOf[RxExecuter])
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      assert(actualExecuter.runner.isInstanceOf[CompositeChunkRunner])
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).toSet === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
      
      actualExecuter.jobFilter shouldBe a[DbBackedJobFilter]
      
      actualExecuter.jobFilter.asInstanceOf[DbBackedJobFilter].dao shouldBe wiring.dao
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
  
  test("DRM execution, run everything") {
    def doTest(drmSystem: DrmSystem): Unit = {
      val wiring = appWiring(cliConf(s"${toFlag(drmSystem)} --conf $confFileForUger --run-everything $exampleFile"))
      
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.isInstanceOf[AppWiring.TerminableExecuter])
      
      assert(wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.isInstanceOf[RxExecuter])
      
      val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
      
      assert(actualExecuter.runner.isInstanceOf[CompositeChunkRunner])
      
      val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
      
      assert(runner.components.map(_.getClass).toSet === Set(classOf[AsyncLocalChunkRunner], classOf[DrmChunkRunner]))
      
      actualExecuter.asInstanceOf[RxExecuter].jobFilter shouldBe JobFilter.RunEverything
    }
    
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Lsf)
  }
}
