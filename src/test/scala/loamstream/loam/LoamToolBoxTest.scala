package loamstream.loam

import loamstream.LEnv
import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.loam.LoamToolBoxTest.Results
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.model.AST
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.{Hit, Shot}
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
class LoamToolBoxTest extends FunSuite {
  val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
  val executer = ChunkedExecuter.default

  def getResults(code: String): Results = {
    val compileResult = compiler.compile(code)
    val env = compileResult.envOpt.get
    val graph = compileResult.graphOpt.get.withEnv(env)
    val mapping = LoamGraphAstMapper.newMapping(graph)
    val toolBox = LoamToolBox(env)
    //    assert(mapping.rootAsts.size == 1)
    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_++_)
    val jobResults = executer.execute(executable)
    Results(env, graph, mapping, jobResults)
  }

  test("Simple toy pipeline using mkdir, rmdir.") {
    val code =
      """
        |val dir = store[Path]
        |cmd"mkdir $dir"
        |cmd"rmdir $dir"
      """.stripMargin
    val results = getResults(code)
    assert(results.allJobResultsAreSuccess)
    assert(results.jobResults.size === 2)
    assert(results.mapping.rootAsts.size === 1)
    assert(results.mapping.rootTools.size === 1)
  }

}

object LoamToolBoxTest {

  case class Results(env: LEnv, graph: LoamGraph, mapping: LoamGraphAstMapping,
                     jobResults: Map[LJob, Shot[LJob.Result]]) {
    def allJobResultsAreSuccess : Boolean = jobResults.values.forall({
      case Hit(CommandSuccess(_)) => true
      case _ => false
    })

  }

}