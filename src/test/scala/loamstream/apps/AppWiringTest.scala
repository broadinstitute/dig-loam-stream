package loamstream.apps

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.cli.Conf
import org.scalatest.Matchers
import loamstream.db.slick.SlickLoamDao
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.uger.UgerChunkRunner
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.HashingStrategy
import loamstream.cli.Intent
import loamstream.TestHelpers
import loamstream.db.slick.DbDescriptor

/**
 * @author clint
 * Nov 14, 2016
 */
final class AppWiringTest extends FunSuite with Matchers {
  private val exampleFile = "src/examples/loam/cp.loam"
  
  private val exampleFilePath = Paths.get(exampleFile)
  
  private val confFileForUger = Paths.get("src/test/resources/for-uger.conf")
  
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
  
  test("Uger execution also, db-backed") {
    
    val wiring = appWiring(cliConf(s"--conf $confFileForUger $exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]

    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    actualExecuter.runner shouldBe a[CompositeChunkRunner]
    
    val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
    
    //NB: Use asInstanceOf because a[T] doesn't play nice with 'should contain' :\
    assert(runner.components.exists(_.isInstanceOf[AsyncLocalChunkRunner]))
    assert(runner.components.exists(_.isInstanceOf[UgerChunkRunner]))
    assert(runner.components.size === 2)
    
    actualExecuter.jobFilter shouldBe a[DbBackedJobFilter]
    
    actualExecuter.jobFilter.asInstanceOf[DbBackedJobFilter].dao shouldBe wiring.dao
  }
  
  test("Uger execution, run everything") {
    val wiring = appWiring(cliConf(s"--conf $confFileForUger --run-everything $exampleFile"))
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate shouldBe a[RxExecuter]
    
    val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate.asInstanceOf[RxExecuter]
    
    actualExecuter.runner shouldBe a[CompositeChunkRunner]
    
    val runner = actualExecuter.runner.asInstanceOf[CompositeChunkRunner]
    
    //NB: Use asInstanceOf because a[T] doesn't play nice with 'should contain' :\
    assert(runner.components.exists(_.isInstanceOf[AsyncLocalChunkRunner]))
    assert(runner.components.exists(_.isInstanceOf[UgerChunkRunner]))
    assert(runner.components.size === 2)
    
    actualExecuter.asInstanceOf[RxExecuter].jobFilter shouldBe JobFilter.RunEverything
  }
}
