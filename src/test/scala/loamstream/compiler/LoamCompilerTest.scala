package loamstream.compiler

import java.nio.file.Paths

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.tools.core.LCoreEnv
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
class LoamCompilerTest extends FunSuite {
  test("Testing compilation of various code fragments (only works after running captureSbtClasspath).") {
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
}
