package loamstream

import java.nio.file.Paths
import org.scalatest.FunSuite
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject

/**
  * @author oliver
  * @author clint
  * 
  * Created Jun 27, 2016
  * Done over: Feb 27, 2017
  * 
  */
final class LoamRepositoryTest extends FunSuite {
  test("All examples compile") {
    val examples = Paths.get("src/examples/loam")
    
    val loamFiles = examples.toFile.listFiles.filter(_.getName.endsWith(".loam")).map(_.toPath)
    
    val loamEngine = TestHelpers.loamEngine
    
    val compiler = new LoamCompiler
    
    for (loamFile <- loamFiles) {
      val scriptShot = loamEngine.loadFile(loamFile)
      
      assert(scriptShot.nonEmpty, scriptShot.message)
      
      val script = scriptShot.get
      
      val compileResult = compiler.compile(LoamProject(TestHelpers.config, script))
      
      val message = s"${script}\n${compileResult.report}"
      
      assert(compileResult.isSuccess, message)
      assert(compileResult.isClean, message)
    }
  }
}
