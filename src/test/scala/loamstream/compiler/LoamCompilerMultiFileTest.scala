package loamstream.compiler

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.loam.LoamScript
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/19/2016.
  */
final class LoamCompilerMultiFileTest extends FunSuite {
  test("Compile project with two scripts, individual import") {
    val codeLib =
      """
        |val answer = 42
      """.stripMargin
    val codeMain =
      """
        |import lib.answer
        |cmd"echo The answer is $answer"
      """.stripMargin
    val scriptMain = LoamScript("main", codeMain)
    val scriptLib = LoamScript("lib", codeLib)
    val project = LoamProject(scriptMain, scriptLib)
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val compileResults = compiler.compile(project)
    assert(compileResults.isSuccess)
    assert(compileResults.isClean)
    assert(compileResults.contextOpt.get.graph.tools.size === 1)
  }

  test("Compile project with two scripts, wild-card import") {
    val codeLib =
      """
        |val answer = 42
      """.stripMargin
    val codeMain =
      """
        |import lib._
        |cmd"echo The answer is $answer"
      """.stripMargin
    val scriptMain = LoamScript("main", codeMain)
    val scriptLib = LoamScript("lib", codeLib)
    val project = LoamProject(scriptMain) + scriptLib
    val compiler = new LoamCompiler(OutMessageSink.NoOp)
    val compileResults = compiler.compile(project)
    assert(compileResults.isSuccess)
    assert(compileResults.isClean)
    assert(compileResults.contextOpt.get.graph.tools.size === 1)
  }

}
