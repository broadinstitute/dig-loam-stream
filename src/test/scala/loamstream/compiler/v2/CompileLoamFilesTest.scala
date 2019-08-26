package loamstream.compiler.v2

import org.scalatest.FunSuite
import loamstream.util.Files
import loamstream.TestHelpers
import loamstream.util.Paths
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.compiler.LoamEngine
import loamstream.model.execute.RxExecuter

/**
 * @author clint
 * Jul 31, 2019
 */
final class CompileLoamFilesTest extends FunSuite {
  test("compileV2") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      
      import TestHelpers.path
      import Paths.Implicits._
      
      val file0 = path("src/test/scala/loamstream/compiler/v2/File0.scala")
      val file1 = path("src/test/scala/loamstream/compiler/v2/File1.scala")
      
      Files.writeTo(path("a.txt"))("AAA")
      
      val compiler = LoamCompiler.default
      
      val loamConfig = LoamConfig.fromConfig(ConfigFactory.empty).get
      
      val compilationResult = compiler.compileV2(loamConfig, "loamstream.compiler.v2", file1, file0)
      
      assert(compilationResult.isSuccess)
      
      val graph = compilationResult.asInstanceOf[LoamCompiler.Result.Success].graph
      
      val executable = LoamEngine.toExecutable(graph)
      
      val finalOutput = path("target/c.txt")
      
      finalOutput.toFile.delete()
      
      assert(java.nio.file.Files.exists(finalOutput) === false)
      
      val executionResult = RxExecuter.default.execute(executable)
      
      assert(executionResult.size === 2)
      assert(executionResult.values.forall(_.isSuccess))
      
      assert(java.nio.file.Files.exists(finalOutput) === true)
      assert(Files.readFrom(finalOutput) === "AAA")
    }
  }
}
