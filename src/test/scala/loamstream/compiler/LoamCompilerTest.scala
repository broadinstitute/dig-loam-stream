package loamstream.compiler

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamGraphValidation
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
object LoamCompilerTest {

  object SomeObject

  def classIsLoaded(classLoader: ClassLoader, className: String): Boolean = {
    classLoader.loadClass(className).getName == className
  }
}

final class LoamCompilerTest extends FunSuite {
  test("Testing sanity of classloader used by compiler.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val compilerClassLoader = compiler.compiler.rootClassLoader
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "java.lang.String"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.collection.immutable.Seq"))
    assert(LoamCompilerTest.classIsLoaded(compilerClassLoader, "scala.tools.nsc.Settings"))
  }

  test("Testing compilation of legal code fragment with no settings (saying 'Hello!').") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val code = {
      // scalastyle:off regex
      """
     val hello = "Yo!".replace("Yo", "Hello")
     println(s"A code fragment used to test the Loam compiler says '$hello'")
      """
      // scalastyle:on regex
    }
    val result = compiler.compile(code)
    assert(result.errors === Nil)
    assert(result.warnings === Nil)
  }
  test("Testing that compilation of illegal code fragment causes compile errors.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val code = {
      """
    The enlightened soul is a person who is self-conscious of his "human condition" in his time and historical
    and social setting, and whose awareness inevitably and necessarily gives him a sense of social responsibility.
      """
    }
    val result = compiler.compile(code)
    assert(result.errors.nonEmpty)
  }
  test("Testing sample code toyImpute.loam") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val codeShot = LoamRepository.defaultRepo.load("toyImpute").map(_.content)
    assert(codeShot.nonEmpty)
    val result = compiler.compile(codeShot.get)
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    val graph = result.contextOpt.get.graph
    assert(graph.tools.size === 2)
    assert(graph.stores.size === 4)
    val sources = graph.storeSources.values.toSet
    assert(!sources.forall(_.isInstanceOf[StoreEdge.ToolEdge]))
    assert(sources.collect({ case StoreEdge.ToolEdge(tool) => tool }) == graph.tools)
    val validationIssues = LoamGraphValidation.allRules(graph)
    assert(validationIssues.isEmpty)
  }
}
