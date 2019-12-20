package loamstream.compiler

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.CompilationConfig
import loamstream.loam.LoamGraphValidation
import loamstream.loam.LoamScript

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
    val result = compiler.compile(
        TestHelpers.config, 
        LoamScript("LoamCompilerTestScript1", code), 
        propertiesForLoamCode = Nil)
        
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
    val result = compiler.compile(
        TestHelpers.config, 
        LoamScript("LoamCompilerTestScript2", code), 
        propertiesForLoamCode = Nil)
        
    assert(result.errors.nonEmpty)
  }
  
  test("Testing sample code toyImpute.loam") {
    val compiler = LoamCompiler.default

    val exampleFile = Paths.get("src/examples/loam/toyImpute.loam")

    val scriptAttempt = LoamEngine.loadFile(exampleFile)
        
    assert(scriptAttempt.isSuccess)
    
    val result = compiler.compile(
        project = LoamProject(TestHelpers.config, scriptAttempt.get), 
        propertiesForLoamCode = Nil)
    
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    
    val graph = result.asInstanceOf[LoamCompiler.Result.Success].graph
    
    assert(graph.tools.size === 2)
    assert(graph.stores.size === 4)
    assert(graph.stores.exists(store => !graph.storeProducers.contains(store)))
    assert((graph.stores -- graph.inputStores).forall(store => graph.storeProducers.contains(store)))
    
    val validationIssues = LoamGraphValidation.allRules(graph)
    assert(validationIssues.isEmpty)
  }
}
