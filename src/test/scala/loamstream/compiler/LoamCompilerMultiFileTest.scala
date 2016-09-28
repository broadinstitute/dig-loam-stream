package loamstream.compiler

import loamstream.loam.LoamScript
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/19/2016.
  */
final class LoamCompilerMultiFileTest extends FunSuite {
  def assertCompiledFine(results: LoamCompiler.Result, nStores: Int, nTools: Int): Unit = {
    assert(results.isSuccess)
    assert(results.isClean)
    assert(results.contextOpt.nonEmpty)
    val graph = results.contextOpt.get.graph
    assert(graph.stores.size === nStores)
    assert(graph.tools.size === nTools)
  }

  val scriptValues = LoamScript("values",
    """
      |val greeting = "Hello"
      |val answer = 42
    """.stripMargin)

  test("Compile project with two scripts, individual import") {
    val scriptIndividualImport = LoamScript("individualImport",
      """
        |import values.{answer, greeting}
        |cmd"echo $greeting the answer is $answer"
      """.stripMargin)
    val project = LoamProject(scriptValues, scriptIndividualImport)
    val compiler = new LoamCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
  }

  test("Compile project with two scripts, wild-card import") {
    val scriptWildcardImport = LoamScript("wildcardImport",
      """
        |import values._
        |cmd"echo $greeting the answer is $answer"
      """.stripMargin)
    val project = LoamProject(scriptValues, scriptWildcardImport)
    val compiler = new LoamCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
  }

}
