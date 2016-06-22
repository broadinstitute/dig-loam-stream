package loamstream.loam

import loamstream.compiler.ClientMessageHandler.OutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.Hit
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * LoamStream
  * Created by oliverr on 6/21/2016.
  */
class LoamToolBoxTest extends FunSuite {
  test("Simple toy pipeline using mkdir, rmdir.") {
    val code =
      """
        |val dir = store[Path]
        |cmd"mkdir $dir"
        |cmd"ls $dir"
      """.stripMargin
    val compiler = new LoamCompiler(OutMessageSink.NoOp)(global)
    val compileResult = compiler.compile(code)
    val env = compileResult.envOpt.get
    val graph = compileResult.graphOpt.get.withEnv(env)
    val mapping = LoamGraphAstMapper.newMapping(graph)
    val toolBox = LoamToolBox(env)
    val executer = ChunkedExecuter.default
//    assert(mapping.rootAsts.size == 1)
    val ast = mapping.rootAsts.head
    val executable = toolBox.createExecutable(ast)
    val jobResults = executer.execute(executable)
    assert(jobResults.size === 2)
    // TODO
//    assert(jobResults.keys.forall({
//
//    }))
    assert(jobResults.values.forall({
      case Hit(CommandSuccess(_)) => true
      case _ => false
    }))
  }

}
