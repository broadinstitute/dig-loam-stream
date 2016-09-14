package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.loam.{LoamContext, LoamScript, LoamToolBox}
import loamstream.model.execute.{ChunkedExecuter, LExecutable}
import loamstream.model.jobs.LJob
import loamstream.util.{Loggable, Shot}

import scala.concurrent.ExecutionContext

/**
  * @author clint
  *         date: Jul 8, 2016
  */
trait LoamTestHelpers extends Loggable {

  def compileFile(file: String)(implicit context: ExecutionContext): LoamCompiler.Result = compile(Paths.get(file))

  def compile(path: Path)(implicit context: ExecutionContext): LoamCompiler.Result =
    compile(LoamScript.read(path).get)


  def compile(script: LoamScript)(implicit context: ExecutionContext): LoamCompiler.Result = {

    val compiler = new LoamCompiler(LoggableOutMessageSink(this))

    val compileResults = compiler.compile(script)

    if (!compileResults.isValid) {
      throw new IllegalArgumentException(s"Could not compile '$script': ${compileResults.errors}.")
    }

    compileResults
  }

  def toExecutable(compileResults: LoamCompiler.Result): (LoamGraphAstMapping, LExecutable) = {
    val context: LoamContext = compileResults.contextOpt.get
    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    (mapping, executable)
  }

  def run(executable: LExecutable): Map[LJob, Shot[LJob.Result]] = ChunkedExecuter.default.execute(executable)
}