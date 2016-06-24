package loamstream.loam

import java.nio.file.{Paths, Files => JFiles}

import loamstream.LEnv
import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.loam.LoamToolBoxTest.Results
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.{Files, Hit, Shot}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
class LoamToolBoxTest extends FunSuite {
  val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
  val executer = ChunkedExecuter.default

  def run(code: String): Results = {
    val compileResult = compiler.compile(code)
    val env = compileResult.envOpt.get
    val graph = compileResult.graphOpt.get.withEnv(env)
    val mapping = LoamGraphAstMapper.newMapping(graph)
    val toolBox = LoamToolBox(env)
    //    assert(mapping.rootAsts.size == 1)
    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)
    val jobResults = executer.execute(executable)
    Results(env, graph, mapping, jobResults)
  }

  test("Simple toy pipeline using cp.") {
    val fileIn = Paths.get("fileIn.txt")
    val fileOut1 = Paths.get("fileOut1.txt")
    val fileOut2 = Paths.get("fileOut2.txt")
    val fileOut3 = Paths.get("fileOut3.txt")
    Files.writeTo(fileIn)("Hello World!")
    val code =
      """
        |val fileIn = store[String].from(path("fileIn.txt"))
        |val fileTmp1 = store[String]
        |val fileTmp2 = store[String]
        |val fileOut1 = store[String].to(path("fileOut1.txt"))
        |val fileOut2 = store[String].to(path("fileOut2.txt"))
        |val fileOut3 = store[String].to(path("fileOut3.txt"))
        |cmd"cp $fileIn $fileTmp1"
        |cmd"cp $fileTmp1 $fileTmp2"
        |cmd"cp $fileTmp2 $fileOut1"
        |cmd"cp $fileTmp2 $fileOut2"
        |cmd"cp $fileTmp2 $fileOut3"
      """.stripMargin
    val results = run(code)
    assert(results.allJobResultsAreSuccess)
    assert(results.jobResults.size === 5)
    assert(results.mapping.rootAsts.size === 3)
    assert(results.mapping.rootTools.size === 3)
    assert(JFiles.exists(fileIn))
    assert(JFiles.exists(fileOut1))
    assert(JFiles.exists(fileOut2))
    assert(JFiles.exists(fileOut3))
    JFiles.delete(fileIn)
    JFiles.delete(fileOut1)
    JFiles.delete(fileOut2)
    JFiles.delete(fileOut3)
  }

}

object LoamToolBoxTest {

  case class Results(env: LEnv, graph: LoamGraph, mapping: LoamGraphAstMapping,
                     jobResults: Map[LJob, Shot[LJob.Result]]) {
    def allJobResultsAreSuccess: Boolean = jobResults.values.forall({
      case Hit(CommandSuccess(_)) => true
      case _ => false
    })

  }

}