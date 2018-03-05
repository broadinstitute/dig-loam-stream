package loamstream.compiler

import java.nio.file.Paths

import loamstream.loam.{LoamGraphValidation, LoamScript}
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamProjectContext
import loamstream.util.ValueBox
import loamstream.model.Store

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
    val compiler = new LoamCompiler
    val compilerClassLoader = compiler.compiler.rootClassLoader
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "java.lang.String"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.collection.immutable.Seq"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.tools.nsc.Settings"))
  }

  test("Testing compilation of legal code fragment with no settings (saying 'Hello!').") {
    val compiler = new LoamCompiler
    val code = {
      // scalastyle:off regex
      """
     val hello = "Yo!".replace("Yo", "Hello")
     println(s"A code fragment used to test the Loam compiler says '$hello'")
      """
      // scalastyle:on regex
    }
    val result = compiler.compile(TestHelpers.config, LoamScript("LoamCompilerTestScript1", code))
    assert(result.errors === Nil)
    assert(result.warnings === Nil)
  }
  
  test("Testing that compilation of illegal code fragment causes compile errors.") {
    val settingsWithNoCodeLoggingOnError = LoamCompiler.Settings.default.copy(logCodeOnError = false)
    val compiler = new LoamCompiler(settingsWithNoCodeLoggingOnError)
    val code = {
      """
    The enlightened soul is a person who is self-conscious of his "human condition" in his time and historical
    and social setting, and whose awareness inevitably and necessarily gives him a sense of social responsibility.
      """
    }
    val result = compiler.compile(TestHelpers.config, LoamScript("LoamCompilerTestScript2", code))
    assert(result.errors.nonEmpty)
  }
  
  test("Testing sample code toyImpute.loam") {
    val compiler = new LoamCompiler

    val exampleFile = Paths.get("src/examples/loam/toyImpute.loam")

    val scriptShot = TestHelpers.loamEngine.loadFile(exampleFile)
        
    assert(scriptShot.nonEmpty)
    
    val result = compiler.compile(LoamProject(TestHelpers.config, scriptShot.get))
    
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    val graph = result.contextOpt.get.graph
    assert(graph.tools.size === 2)
    assert(graph.stores.size === 4)
    assert(graph.stores.exists { store =>
      graph.storeLocations.contains(store) && !graph.storeProducers.contains(store)
    })
    assert(graph.stores.forall { store =>
      graph.storeLocations.contains(store) || graph.storeProducers.contains(store)
    })
    val validationIssues = LoamGraphValidation.allRules(graph)
    assert(validationIssues.isEmpty)
  }
  
  test("Result.toGraphSource") {
    import LoamCompiler.Result.toGraphSource
    
    //No LoamProjectContext
    assert(toGraphSource(None) === GraphSource.Empty)
    
    val (g0, g1, g2) = GraphQueueTest.makeTestGraphs()
    
    import TestHelpers.config
    
    //LoamProjectContext exists with empty graph queue
    {
      val ctx = new LoamProjectContext(config, ValueBox(g1), GraphQueue.empty)
      
      val source = toGraphSource(Some(ctx))
      
      assert(source.iterator.hasNext === false)
    }
    
    //LoamProjectContext exists with non-empty graph queue
    {
      val ctx = new LoamProjectContext(config, ValueBox(g0), GraphQueue(() => g1, () => g2))
      
      val source = toGraphSource(Some(ctx))
      
      assert(source.iterator.toSeq.map(_.apply()) === Seq(g1, g2))
    }
  }
}
