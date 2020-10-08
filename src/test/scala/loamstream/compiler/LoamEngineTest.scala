package loamstream.compiler

import java.nio.file.{ Files => JFiles }

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.conf.ExecutionConfig
import loamstream.loam.LoamCmdTool
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.Files
import loamstream.util.Paths
import loamstream.model.execute.LocalSettings
import loamstream.loam.LoamLoamScript
import loamstream.conf.LsSettings


/**
 * @author clint
 * Feb 27, 2018
 */
final class LoamEngineTest extends FunSuite {
  import loamstream.TestHelpers.{ config, path } 
  
  private val engine: LoamEngine = TestHelpers.loamEngine
  
  private val cpDotLoam = path("src/examples/loam/cp.loam")
  private val firstDotLoam = path("src/examples/loam/first.loam")
  
  test("loadFile") {
    val script = engine.loadFile(cpDotLoam).get
    
    assert(script.name === "cp")
    assert(script.subPackage === None)
    assert(script.asInstanceOf[LoamLoamScript].code === Files.readFrom(cpDotLoam))
  }
  
  test("scriptsFrom") {
    //Empty list of files
    assert(engine.scriptsFrom(Nil).get === Nil)
    
    //One file
    {
      val scripts = engine.scriptsFrom(Seq(cpDotLoam)).get
      
      assert(scripts.size === 1)
      
      val cpScript = scripts.head
    
      assert(cpScript.name === "cp")
      assert(cpScript.subPackage === None)
      assert(cpScript.asInstanceOf[LoamLoamScript].code === Files.readFrom(cpDotLoam))
    }

    //Multiple files
    val loamFiles = Seq(cpDotLoam, firstDotLoam)
        
    val scripts = engine.scriptsFrom(loamFiles).get
    
    val cpScript = scripts.find(_.name == "cp").get
    
    val firstScript = scripts.find(_.name == "first").get
    
    assert(cpScript.name === "cp")
    assert(cpScript.subPackage === None)
    assert(cpScript.asInstanceOf[LoamLoamScript].code === Files.readFrom(cpDotLoam))
    
    assert(firstScript.name === "first")
    assert(firstScript.subPackage === None)
    assert(firstScript.asInstanceOf[LoamLoamScript].code === Files.readFrom(firstDotLoam))
  }
  
  test("compile") {
    val script = engine.loadFile(firstDotLoam).get
    
    val project = LoamProject(config, LsSettings.noCliConfig, Set(script))
    
    val results @ LoamCompiler.Result.Success(warnings, infos, graph) = engine.compile(project)
    
    assert(results.errors === Nil)
    assert(infos === Nil)
    assert(warnings === Nil)
    
    assert(graph.stores.size === 2)
    assert(graph.tools.size === 1)
  }
  
  test("compileFiles") {
    val results @ LoamCompiler.Result.Success(warnings, infos, graph) = {
      engine.compileFiles(Seq(cpDotLoam, firstDotLoam)).get
    }
    
    assert(results.errors === Nil)
    assert(infos === Nil)
    assert(warnings === Nil)
    
    assert(graph.stores.size === 8)
    assert(graph.tools.size === 6)
  }
  
  test("run") {
    val tempDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    import Paths.Implicits._

    val inputFile = tempDir / "A"
    val outputFile = tempDir / "B"
    
    Files.writeTo(inputFile)("XYZ")
    
    val graph = TestHelpers.makeGraph { implicit context =>
      import loamstream.loam.LoamSyntax._

      val input = store(inputFile).asInput
      val output = store(outputFile)
      
      cmd"cp $input $output".in(input).out(output)
    }
    
    assert(JFiles.exists(inputFile))
    assert(Files.readFrom(inputFile) === "XYZ")
    
    assert(JFiles.exists(outputFile) === false)
    
    val results = engine.run(graph)
    
    assert(results.size === 1)
    
    assert(results.values.head.isSuccess === true)
    
    assert(JFiles.exists(inputFile))
    assert(Files.readFrom(inputFile) === "XYZ")
    
    assert(JFiles.exists(outputFile))
    assert(Files.readFrom(outputFile) === "XYZ")
  }
  
  test("listJobsThatCouldRun") {
    
    val tempDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val outputFile = tempDir.resolve("dryRunOutput")
    
    val localConfig = config.copy(executionConfig = ExecutionConfig.default)
    
    val localEngine = LoamEngine.default(localConfig, LsSettings.noCliConfig)
    
    def job(commandLine: String) = {
      CommandLineJob(commandLineString = commandLine, initialSettings = LocalSettings)
    }
    
    val j0 = job("foo")
    val j1 = job("bar")
    val j2 = job("baz")
    
    val jobs = Seq(j0, j1, j2)
    
    assert(JFiles.exists(outputFile) === false)
    
    localEngine.listJobsThatCouldRun(jobs, outputFile)
    
    assert(JFiles.exists(outputFile) === true)
    
    val actuallyOutput = Files.getLines(outputFile)
    
    assert(actuallyOutput(0).endsWith(j0.toString))
    assert(actuallyOutput(1).endsWith(j1.toString))
    assert(actuallyOutput(2).endsWith(j2.toString))
  }
}
