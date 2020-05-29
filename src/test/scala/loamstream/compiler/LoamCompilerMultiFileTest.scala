package loamstream.compiler

import loamstream.loam.{LoamCmdTool, LoamScript}
import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamLoamScript
import loamstream.loam.ScriptType
import loamstream.loam.ScalaLoamScript
import loamstream.util.code.ScalaId

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

  private def valuesScript(scriptType: ScriptType): LoamScript = {
    val name = "values"
    val code = """|val greeting = "Hello"
                  |val answer = 42""".stripMargin
                  
    scriptType match {
      case ScriptType.Loam => LoamLoamScript(name, code)
      case ScriptType.Scala => ScalaLoamScript(name, LoamCompilerTest.wrapInLoamFile(name, code))
    }
  }
  
  test("Full name instead of import") {
    def doTest(scriptType: ScriptType): Unit = {
      val scriptIndividualImport = LoamLoamScript("individualImport",
        """
          |cmd"echo ${values.greeting} the answer is ${values.answer}"
        """.stripMargin)
      val project = LoamProject(TestHelpers.config, valuesScript(scriptType), scriptIndividualImport)

      val compileResults = LoamCompiler.default.compile(project)
      assertCompiledFine(compileResults, 0, 1)
      assertEchoCommand(compileResults)
    }
    
    doTest(ScriptType.Loam)
    doTest(ScriptType.Scala)
  }

  test("Individual import") {
    def doTest(scriptType: ScriptType): Unit = {
      val scriptIndividualImport = LoamLoamScript("individualImport",
        """
          |import values.{answer, greeting}
          |cmd"echo $greeting the answer is $answer"
        """.stripMargin)
      val project = LoamProject(TestHelpers.config, valuesScript(scriptType), scriptIndividualImport)

      val compileResults = LoamCompiler.default.compile(project)
      assertCompiledFine(compileResults, 0, 1)
      assertEchoCommand(compileResults)
    }
    
    doTest(ScriptType.Loam)
    doTest(ScriptType.Scala)
  }

  test("Renaming import") {
    def doTest(scriptType: ScriptType): Unit = {
      val scriptIndividualImport = LoamLoamScript("individualImport",
        """
          |import values.{answer => answerToTheGreatQuestion, greeting => casualGreeting}
          |cmd"echo $casualGreeting the answer is $answerToTheGreatQuestion"
        """.stripMargin)
      val project = LoamProject(TestHelpers.config, valuesScript(scriptType), scriptIndividualImport)

      val compileResults = LoamCompiler.default.compile(project)
      assertCompiledFine(compileResults, 0, 1)
      assertEchoCommand(compileResults)
    }
    
    doTest(ScriptType.Loam)
    doTest(ScriptType.Scala)
  }

  test("Wild-card import") {
    def doTest(scriptType: ScriptType): Unit = {
      val scriptWildcardImport = LoamLoamScript("wildcardImport",
        """
          |import values._
          |cmd"echo $greeting the answer is $answer"
        """.stripMargin)
      val project = LoamProject(TestHelpers.config, valuesScript(scriptType), scriptWildcardImport)

      val compileResults = LoamCompiler.default.compile(project)
      assertCompiledFine(compileResults, 0, 1)
      assertEchoCommand(compileResults)
    }
    
    doTest(ScriptType.Loam)
    doTest(ScriptType.Scala)
  }

  test("Diamond import relationship diagram") {
    def doTest(valuesScriptType: ScriptType, combinerScriptType: ScriptType): Unit = {
      val combinerScriptName = "combiner"
      
      val combinerScriptCode = """
            |import greetingForwarder.copyOfGreeting
            |import answerForwarder.copyOfAnswer
            |cmd"echo $copyOfGreeting the answer is $copyOfAnswer"
            |""".stripMargin.trim
      
      val combinerScript: LoamScript = combinerScriptType match {
        case ScriptType.Loam => LoamLoamScript(combinerScriptName, combinerScriptCode)
        case ScriptType.Scala => {
          ScalaLoamScript(
              combinerScriptName,
              LoamCompilerTest.wrapInLoamFile(combinerScriptName, combinerScriptCode))
        }
      }
      
      val scripts: Set[LoamScript] = Set(
        valuesScript(valuesScriptType),
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
        combinerScript)
          
      val project = LoamProject(TestHelpers.config, scripts)

      val compileResults = LoamCompiler.default.compile(project)
      assertCompiledFine(compileResults, 0, 1)
      assertEchoCommand(compileResults)
    }
    
    doTest(ScriptType.Loam, ScriptType.Loam)
    doTest(ScriptType.Loam, ScriptType.Scala)
    doTest(ScriptType.Scala, ScriptType.Scala)
    doTest(ScriptType.Scala, ScriptType.Loam)
  }
}
