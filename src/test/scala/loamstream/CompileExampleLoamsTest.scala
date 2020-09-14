package loamstream

import java.nio.file.Paths
import org.scalatest.FunSuite
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject
import loamstream.loam.ScriptType
import loamstream.conf.LsSettings

/**
  * @author oliver
  * @author clint
  * 
  * Created Jun 27, 2016
  * Done over: Feb 27, 2017
  * 
  */
final class CompileExampleLoamsTest extends FunSuite {
  test("All .loam examples compile") {
    doCompileExamplesTest(ScriptType.Loam)
  }
  
  test("All .scala examples compile") {
    doCompileExamplesTest(ScriptType.Scala)
  }
  
  private def doCompileExamplesTest(scriptType: ScriptType): Unit = {
    val examples = Paths.get(s"src/examples/${scriptType.name}")
    
    val loamFiles = examples.toFile.listFiles.filter(_.getName.endsWith(scriptType.suffix)).map(_.toPath)
    
    val loamEngine = TestHelpers.loamEngine
    
    for (loamFile <- loamFiles) {
      val scriptAttempt = loamEngine.loadFile(loamFile)
      
      require(scriptAttempt.isSuccess, scriptAttempt.failed.get.getMessage)
      
      val script = scriptAttempt.get
      
      val compileResult = loamEngine.compile(LoamProject(TestHelpers.configWithUger, LsSettings.noCliConfig, script))
      
      val message = s"; Compilation failed for $loamFile:\n${compileResult.report}"
      
      assert(compileResult.isSuccess, message)
      assert(compileResult.isClean, message)
    }
  }
}
