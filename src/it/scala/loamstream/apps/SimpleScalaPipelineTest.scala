package loamstream.apps

import org.scalatest.FunSuite
import loamstream.IntegrationTestHelpers
import loamstream.util.Files
import loamstream.compiler.LoamProject
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.loam.LoamScript
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamCompiler
import loamstream.model.execute.RxExecuter

/**
 * @author clint
 * May 29, 2020
 */
final class SimpleScalaPipelineTest extends FunSuite {
  test("Simple .scala pipeline") {
    import loamstream.util.Paths.Implicits.PathHelpers
    
    IntegrationTestHelpers.withWorkDirUnderTarget(Some(getClass.getSimpleName)) { workDir =>
      val valuesFile = workDir / "values.scala"
      val commandsFile = workDir / "commands.scala"
      
      val greetingOutput = workDir / "greeting-output.txt"
      val cpOutput = workDir / "cp-output.txt"
      
      Files.writeTo(valuesFile) {
        """|object values extends loamstream.LoamFile {
           |  val n: Int = 42
           |  val name: String = "Foo"
           |}""".stripMargin
      }
      
      Files.writeTo(commandsFile) {
        s"""|object commands extends loamstream.LoamFile {
            |  val greetingOutput = store("${greetingOutput}")
            |  val cpOutput = store("${cpOutput}")
            |
            |  val greeting = s"Hello $${values.name}!"
            |
            |  cmd"echo $${greeting} N = $${values.n} > $$greetingOutput".out(greetingOutput)
            |
            |  cmd"cp $$greetingOutput $$cpOutput".in(greetingOutput).out(cpOutput)
            |}""".stripMargin
      }
      
      //NB: "Empty" config
      val config = LoamConfig.fromConfig(ConfigFactory.parseString("{}")).get
      
      val scripts = Seq(valuesFile, commandsFile).flatMap(LoamScript.read(_).toOption)
      
      val project = LoamProject(config, scripts)
      
      val compilationResult = LoamCompiler.default.compile(project)
      
      assert(compilationResult.isSuccess)
      assert(compilationResult.isClean)
      
      val graph = compilationResult.asInstanceOf[LoamCompiler.Result.Success].graph
      
      val executable = LoamEngine.toExecutable(graph)
      
      val executionResult = RxExecuter.default.execute(executable)
      
      val expected = s"Hello Foo! N = 42${System.lineSeparator}"
      
      assert(Files.readFrom(cpOutput) === expected)
      
      assert(executionResult.values.forall(_.isSuccess))
      
      assert(executionResult.size === 2)
    }
  }
}
