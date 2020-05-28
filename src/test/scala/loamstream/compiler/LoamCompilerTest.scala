package loamstream.compiler

import java.nio.file.Paths

import loamstream.loam.{LoamGraphValidation, LoamScript}
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamProjectContext
import loamstream.util.ValueBox
import loamstream.model.Store
import loamstream.conf.CompilationConfig
import loamstream.loam.LoamLoamScript

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
object LoamCompilerTest {

  object SomeObject

  private def classIsLoaded(classLoader: ClassLoader, className: String): Boolean = {
    classLoader.loadClass(className).getName == className
  }
}

final class LoamCompilerTest extends FunSuite {
  test("Testing sanity of classloader used by compiler.") {
    val compiler = LoamCompiler.default
    val compilerClassLoader = compiler.compiler.rootClassLoader
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "java.lang.String"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.collection.immutable.Seq"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.tools.nsc.Settings"))
  }

  test("Testing compilation of legal code fragment with no settings (saying 'Hello!').") {
    val compiler = LoamCompiler.default
    val code = {
      // scalastyle:off regex
      """
     val hello = "Yo!".replace("Yo", "Hello")
     println(s"A code fragment used to test the Loam compiler says '$hello'")
      """
      // scalastyle:on regex
    }
    val result = compiler.compile(TestHelpers.config, LoamLoamScript("LoamCompilerTestScript1", code))
    assert(result.errors === Nil)
    assert(result.warnings === Nil)
  }
  
  test("Testing that compilation of illegal code fragment causes compile errors.") {
    val settingsWithNoCodeLoggingOnError = LoamCompiler.Settings.default.copy(logCodeOnError = false)
    val compiler = new LoamCompiler(CompilationConfig.default, settingsWithNoCodeLoggingOnError)
    val code = {
      """
    The enlightened soul is a person who is self-conscious of his "human condition" in his time and historical
    and social setting, and whose awareness inevitably and necessarily gives him a sense of social responsibility.
      """
    }
    val result = compiler.compile(TestHelpers.config, LoamLoamScript("LoamCompilerTestScript2", code))
    assert(result.errors.nonEmpty)
    assert(result.isSuccess === false)
  }
  
  test("Testing sample code toyImpute.loam") {
    val compiler = LoamCompiler.default

    val exampleFile = Paths.get("src/examples/loam/toyImpute.loam")

    val scriptAttempt = TestHelpers.loamEngine.loadFile(exampleFile)
        
    assert(scriptAttempt.isSuccess, s"Expected to find $exampleFile, but got ${scriptAttempt.get}")
    
    val result = compiler.compile(LoamProject(TestHelpers.config, scriptAttempt.get))
    
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    
    val graph = result match {
      case s: LoamCompiler.Result.Success => s.graph
      case f: LoamCompiler.Result.FailureDueToException => throw f.throwable
      case r => fail(s"Unexpected result: $r")
    }
    
    assert(graph.tools.size === 2)
    assert(graph.stores.size === 4)
    assert(graph.stores.exists(store => !graph.storeProducers.contains(store)))
    assert((graph.stores -- graph.inputStores).forall(store => graph.storeProducers.contains(store)))
    
    val validationIssues = LoamGraphValidation.allRules(graph)
    assert(validationIssues.isEmpty)
  }
}
