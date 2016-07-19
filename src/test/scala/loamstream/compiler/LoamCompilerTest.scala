package loamstream.compiler

import java.nio.file.Paths

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.loam.LoamGraphValidation
import loamstream.tools.core.LCoreEnv
import loamstream.util.SourceUtils
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
  test("Testing resolution ot type name.") {
    assert(SourceUtils.shortTypeName[LoamCompilerTest] === "LoamCompilerTest")
    assert(SourceUtils.fullTypeName[LoamCompilerTest] === "loamstream.compiler.LoamCompilerTest")
    assert(SourceUtils.shortTypeName[LoamCompilerTest.SomeObject.type] === "SomeObject")
    assert(SourceUtils.fullTypeName[LoamCompilerTest.SomeObject.type] ===
      "loamstream.compiler.LoamCompilerTest.SomeObject")
  }

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
    assert(result.envOpt.nonEmpty)
    val env = result.envOpt.get
    assert(env.size === 0)
  }
  test("Testing compilation of legal code fragment with five settings.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val code = {
      """
    genotypesId := "myImportantGenotypes"
    sampleFilePath := path("/some/path/to/file")
    pcaProjectionsFilePath := tempFile("pca", "pjs")
    klustaKwikKonfig :=
      loamstream.tools.klusta.KlustaKwikKonfig(path("/usr/local/bin/klusta"), "stuff")
    case class Squirrel(name: String)
    val favoriteSquirrel = key[Squirrel]
    favoriteSquirrel := Squirrel("Tom")
      """
    }

    val result = compiler.compile(code)

    assert(result.errors === Nil) //NB: Compare with Nil for better failure messages
    assert(result.warnings === Nil) //NB: Compare with Nil for better failure messages

    val env = result.envOpt.get

    assert(env(LCoreEnv.Keys.genotypesId) === "myImportantGenotypes")
    assert(env(LCoreEnv.Keys.sampleFilePath)() === Paths.get("/some/path/to/file"))
    assert(env.get(LCoreEnv.Keys.pcaProjectionsFilePath).nonEmpty)
    assert(env.get(LCoreEnv.Keys.klustaKwikKonfig).nonEmpty)
    assert(env.size === 5)
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
    assert(result.envOpt.isEmpty)
  }
  test("Testing sample code toyImpute.loam") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val codeShot = LoamRepository.defaultRepo.load("toyImpute").map(_.content)
    assert(codeShot.nonEmpty)
    val result = compiler.compile(codeShot.get)
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    val graph = result.graphOpt.get
    assert(graph.tools.size === 2)
    assert(graph.stores.size === 4)
    val sources = graph.storeSources.values.toSet
    assert(!sources.forall(_.isInstanceOf[StoreEdge.ToolEdge]))
    assert(sources.collect({ case StoreEdge.ToolEdge(tool) => tool }) == graph.tools)
    val validationIssues = LoamGraphValidation.allRules(graph)
    assert(validationIssues.isEmpty)
  }
}
