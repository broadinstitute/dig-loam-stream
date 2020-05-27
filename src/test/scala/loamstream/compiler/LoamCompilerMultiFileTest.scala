package loamstream.compiler

import loamstream.loam.{LoamCmdTool, LoamScript}
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamLoamScript

/**
  * LoamStream
  * Created by oliverr on 9/19/2016.
  */
final class LoamCompilerMultiFileTest extends FunSuite {
  private def assertCompiledFine(results: LoamCompiler.Result, nStores: Int, nTools: Int): Unit = {
    assert(results.isSuccess, results.report)
    assert(results.isClean, results.report)
    assert(results.isInstanceOf[LoamCompiler.Result.Success], results.report)

    val graph = results.asInstanceOf[LoamCompiler.Result.Success].graph
    
    assert(graph.stores.size === nStores)
    assert(graph.tools.size === nTools)
  }

  private def assertEchoCommand(results: LoamCompiler.Result): Unit = {
    assert(results.isInstanceOf[LoamCompiler.Result.Success])
    
    val graph = results.asInstanceOf[LoamCompiler.Result.Success].graph
    
    assert(graph.tools.size === 1)
    
    val tool = graph.tools.head
    
    assert(tool.isInstanceOf[LoamCmdTool])
    
    val cmdTool = tool.asInstanceOf[LoamCmdTool]
    
    assert(cmdTool.tokens.size === 1)
    assert(cmdTool.tokens.head.toString() === "echo Hello the answer is 42")
  }

  private def createNewCompiler: LoamCompiler = LoamCompiler.default

  private val scriptValues = LoamLoamScript("values",
    """
      |val greeting = "Hello"
      |val answer = 42
    """.stripMargin)

  test("Full name instead of import") {
    val scriptIndividualImport = LoamLoamScript("individualImport",
      """
        |cmd"echo ${values.greeting} the answer is ${values.answer}"
      """.stripMargin)
    val project = LoamProject(TestHelpers.config, scriptValues, scriptIndividualImport)
    val compiler = createNewCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
    assertEchoCommand(compileResults)
  }

  test("Individual import") {
    val scriptIndividualImport = LoamLoamScript("individualImport",
      """
        |import values.{answer, greeting}
        |cmd"echo $greeting the answer is $answer"
      """.stripMargin)
    val project = LoamProject(TestHelpers.config, scriptValues, scriptIndividualImport)
    val compiler = createNewCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
    assertEchoCommand(compileResults)
  }

  test("Renaming import") {
    val scriptIndividualImport = LoamLoamScript("individualImport",
      """
        |import values.{answer => answerToTheGreatQuestion, greeting => casualGreeting}
        |cmd"echo $casualGreeting the answer is $answerToTheGreatQuestion"
      """.stripMargin)
    val project = LoamProject(TestHelpers.config, scriptValues, scriptIndividualImport)
    val compiler = createNewCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
    assertEchoCommand(compileResults)
  }

  test("Wild-card import") {
    val scriptWildcardImport = LoamLoamScript("wildcardImport",
      """
        |import values._
        |cmd"echo $greeting the answer is $answer"
      """.stripMargin)
    val project = LoamProject(TestHelpers.config, scriptValues, scriptWildcardImport)
    val compiler = createNewCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
    assertEchoCommand(compileResults)
  }

  test("Diamond import relationship diagram") {
    val scripts = Set(scriptValues,
      LoamLoamScript("greetingForwarder",
        """
          |import values.greeting
          |val copyOfGreeting = greeting
        """.stripMargin),
      LoamLoamScript("answerForwarder",
        """
          |import values.answer
          |val copyOfAnswer = answer
        """.stripMargin),
      LoamLoamScript("combiner",
        """
          |import greetingForwarder.copyOfGreeting
          |import answerForwarder.copyOfAnswer
          |cmd"echo $copyOfGreeting the answer is $copyOfAnswer"
        """.stripMargin))
    val project = LoamProject(TestHelpers.config, scripts)
    val compiler = createNewCompiler
    val compileResults = compiler.compile(project)
    assertCompiledFine(compileResults, 0, 1)
    assertEchoCommand(compileResults)
  }
}
