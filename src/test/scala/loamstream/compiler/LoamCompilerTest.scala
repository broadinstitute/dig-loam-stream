package loamstream.compiler

import java.io.File
import java.nio.file.Paths

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.tools.core.LCoreEnv
import loamstream.util.{ClassPathFinder, SourceUtils, StringUtils}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
object LoamCompilerTest {

  object SomeObject

}

class LoamCompilerTest extends FunSuite {
  test("Testing resolution ot type name.") {
    assert(SourceUtils.shortTypeName[LoamCompilerTest] === "LoamCompilerTest")
    assert(SourceUtils.fullTypeName[LoamCompilerTest] === "loamstream.compiler.LoamCompilerTest")
    assert(SourceUtils.shortTypeName[LoamCompilerTest.SomeObject.type] === "SomeObject")
    assert(SourceUtils.fullTypeName[LoamCompilerTest.SomeObject.type] ===
      "loamstream.compiler.LoamCompilerTest.SomeObject")
  }
  test("Testing sanity of classpath obtained from classloader.") {
    val classpath = ClassPathFinder.getClassPath(this)
    assert(classpath.split(File.pathSeparator).length > 5)
    assert(classpath.contains("scala-library"))
    assert(classpath.contains("scala-compiler"))
    assert(classpath.contains("scala-reflect"))
    assert(classpath.contains("htsjdk"))
    assert(classpath.contains("logback"))
  }
  test("Testing compilation of legal code fragment with no settings.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val code =
      """
     val hello = "Hello!"
     println(hello.replace("!", "?").size)
      """
    val result = compiler.compile(code)
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    assert(result.envOpt.nonEmpty)
    val env = result.envOpt.get
    assert(env.size === 0)
  }
  test("Testing compilation of legal code fragment with five settings.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val code =
      """
    genotypesId := "myImportantGenotypes"
    sampleFilePath := path("/some/path/to/file")
    pcaProjectionsFilePath := tempFile("pca", "pjs")
    klustaKwikKonfig :=
      loamstream.tools.klusta.KlustaKwikKonfig(path("/usr/local/bin/klusta"), "stuff")
    case class Squirrel(name: String)
    val favoriteSquirrel = Key[Squirrel]("My favourite Squirrel")
    favoriteSquirrel := Squirrel("Tom")
      """
    val result = compiler.compile(code)
    assert(result.errors.isEmpty)
    assert(result.warnings.isEmpty)
    assert(result.envOpt.nonEmpty)
    val env = result.envOpt.get
    assert(env(LCoreEnv.Keys.genotypesId) === "myImportantGenotypes")
    assert(env(LCoreEnv.Keys.sampleFilePath)() === Paths.get("/some/path/to/file"))
    assert(env.get(LCoreEnv.Keys.pcaProjectionsFilePath).nonEmpty)
    assert(env.get(LCoreEnv.Keys.klustaKwikKonfig).nonEmpty)
    assert(env.size === 5)
  }
  test("Testing that compilation of illegal code fragment causes compile errors.") {
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val code =
      """
    The enlightened soul is a person who is self-conscious of his "human condition" in his time and historical
    and social setting, and whose awareness inevitably and necessarily gives him a sense of social responsibility.
      """
    val result = compiler.compile(code)
    assert(result.errors.nonEmpty)
    assert(result.envOpt.isEmpty)
  }
}
