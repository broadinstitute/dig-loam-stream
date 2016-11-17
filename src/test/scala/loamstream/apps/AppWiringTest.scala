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

/**
 * @author clint
 * Nov 14, 2016
 */
final class AppWiringTest extends FunSuite with Matchers {
  private val exampleFile = "src/main/loam/examples/cp.loam"
  
  private val exampleFilePath = Paths.get(exampleFile)
  
  private val confFileForUger = Paths.get("src/test/resources/for-uger.conf")
  
  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").toSeq)
  
  test("Local execution, db-backed") {
    val wiring = AppWiring.forLocal(cliConf(s"--backend local $exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]
    wiring.executer shouldBe a[RxExecuter]
    
    wiring.executer.asInstanceOf[RxExecuter].runner shouldBe(RxExecuter.AsyncLocalChunkRunner)
    
    wiring.executer.asInstanceOf[RxExecuter].jobFilter shouldBe a[DbBackedJobFilter]
    
    wiring.executer.asInstanceOf[RxExecuter].jobFilter.asInstanceOf[DbBackedJobFilter].dao shouldBe wiring.dao
  }
  
  test("Local execution, run everything") {
    val wiring = AppWiring.forLocal(cliConf(s"--run-everything --backend local $exampleFile"))
    
    wiring.executer shouldBe a[RxExecuter]
    
    wiring.executer.asInstanceOf[RxExecuter].runner shouldBe(RxExecuter.AsyncLocalChunkRunner)
    
    wiring.executer.asInstanceOf[RxExecuter].jobFilter shouldBe JobFilter.RunEverything
  }
  
  test("Uger execution, db-backed") {
    
    val wiring = AppWiring.forUger(cliConf(s"--conf $confFileForUger --backend uger $exampleFile"))
    
    wiring.dao shouldBe a[SlickLoamDao]

    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate
    
    actualExecuter shouldBe a[RxExecuter]
    
    actualExecuter.asInstanceOf[RxExecuter].runner shouldBe a[UgerChunkRunner]
    
    actualExecuter.asInstanceOf[RxExecuter].jobFilter shouldBe a[DbBackedJobFilter]
    
    actualExecuter.asInstanceOf[RxExecuter].jobFilter.asInstanceOf[DbBackedJobFilter].dao shouldBe wiring.dao
  }
  
  test("Uger execution, run everything") {
    val wiring = AppWiring.forUger(cliConf(s"--conf $confFileForUger --run-everything --backend local $exampleFile"))
    
    wiring.executer shouldBe a[AppWiring.TerminableExecuter]
    
    val actualExecuter = wiring.executer.asInstanceOf[AppWiring.TerminableExecuter].delegate
    
    actualExecuter shouldBe a[RxExecuter]
    
    actualExecuter.asInstanceOf[RxExecuter].runner shouldBe a[UgerChunkRunner]
    
    actualExecuter.asInstanceOf[RxExecuter].jobFilter shouldBe JobFilter.RunEverything
  }
}