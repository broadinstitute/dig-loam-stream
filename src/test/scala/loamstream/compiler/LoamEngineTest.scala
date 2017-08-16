package loamstream.compiler

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngineTest.Fixture
import loamstream.util.{Files, StringUtils}
import org.scalatest.FunSuite
import loamstream.TestHelpers


final class LoamEngineTest extends FunSuite {
  private val engine = LoamEngine.default(TestHelpers.config)

  test("Compile string") {
    val fixture = Fixture.default
    val compileResult = engine.compile(fixture.code)
    fixture.assertCompileResultsLookGood(compileResult)
  }

  test("Compile file") {
    val fixture = Fixture.default
    val file = JFiles.createTempFile("LoamEngineTest", ".loam")
    Files.writeTo(file)(fixture.code)
    val compileResultShot = engine.compile(file)
    assert(compileResultShot.nonEmpty)
    val compileResult = compileResultShot.get
    fixture.assertCompileResultsLookGood(compileResult)
  }

  test("Run string") {
    val fixture = Fixture.default
    fixture.writeFileIn()
    engine.run(fixture.code)
    fixture.assertOutputFilesArePresent()
  }

  test("Run file") {
    val fixture = Fixture.default
    val file = JFiles.createTempFile("LoamEngineTest", ".loam")
    Files.writeTo(file)(fixture.code)
    fixture.writeFileIn()
    engine.runFile(file)
    fixture.assertOutputFilesArePresent()
  }
}

/**
  * LoamStream
  * Created by oliverr on 7/8/2016.
  */
object LoamEngineTest {

  private object Fixture {
    def default: Fixture = {
      val folder = JFiles.createTempDirectory("LoamEngineTest")
      val fileIn = folder.resolve("fileIn.txt")
      Files.writeTo(fileIn)("Hello World!")
      val fileOut1 = folder.resolve("fileOut1.txt")
      val fileOut2 = folder.resolve("fileOut1.txt")
      val fileOut3 = folder.resolve("fileOut1.txt")
      Fixture(fileIn, fileOut1, fileOut2, fileOut3)
    }
  }

  private final case class Fixture(fileIn: Path, fileOut1: Path, fileOut2: Path, fileOut3: Path) {
    def code = {
      val fileInUnescaped = StringUtils.unescapeBackslashes(fileIn.toString)
      val fileOut1Unescaped = StringUtils.unescapeBackslashes(fileOut1.toString)
      val fileOut2Unescaped = StringUtils.unescapeBackslashes(fileOut2.toString)
      val fileOut3Unescaped = StringUtils.unescapeBackslashes(fileOut3.toString)
      s"""
         |val fileIn = store[TXT].at("$fileInUnescaped").asInput
         |val fileTmp1 = store[TXT]
         |val fileTmp2 = store[TXT]
         |val fileOut1 = store[TXT].at("$fileOut1Unescaped")
         |val fileOut2 = store[TXT].at("$fileOut2Unescaped")
         |val fileOut3 = store[TXT].at("$fileOut3Unescaped")
         |cmd"cp $$fileIn $$fileTmp1"
         |cmd"cp $$fileTmp1 $$fileTmp2"
         |cmd"cp $$fileTmp2 $$fileOut1"
         |cmd"cp $$fileTmp2 $$fileOut2"
         |cmd"cp $$fileTmp2 $$fileOut3"
    """.stripMargin
    }

    def assertCompileResultsLookGood(compileResult: LoamCompiler.Result): Unit = {
      assert(compileResult.isSuccess, compileResult)
      assert(compileResult.isClean, compileResult)
      assert(compileResult.contextOpt.get.graph.stores.size == 6, compileResult)
      assert(compileResult.contextOpt.get.graph.tools.size == 5, compileResult)
    }

    def writeFileIn(): Unit = Files.writeTo(fileIn)("Hello World!")

    def assertOutputFilesArePresent(): Unit = {
      assert(JFiles.exists(fileOut1))
      assert(JFiles.exists(fileOut2))
      assert(JFiles.exists(fileOut3))
    }
  }

}
