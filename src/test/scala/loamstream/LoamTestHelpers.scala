package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.{LoamProjectContext, LoamScript, LoamToolBox}
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.model.execute.{Executable, RxExecuter}
import loamstream.model.jobs.{JobState, LJob}
import loamstream.util.Loggable

import scala.concurrent.ExecutionContext

/**
  * @author clint
  *         date: Jul 8, 2016
  */
trait LoamTestHelpers extends Loggable {

  def compileFile(file: String)(implicit context: ExecutionContext): LoamCompiler.Result = compile(Paths.get(file))

  def compile(path: Path)(implicit context: ExecutionContext): LoamCompiler.Result = {
    compile(LoamScript.read(path).get)
  }

  def compile(script: LoamScript)(implicit context: ExecutionContext): LoamCompiler.Result = {

    val compiler = new LoamCompiler(LoamCompiler.Settings.default, LoggableOutMessageSink(this))

    val compileResults = compiler.compile(script)

    if (!compileResults.isValid) {
      throw new IllegalArgumentException(s"Could not compile '$script': ${compileResults.errors}.")
    }

    compileResults
  }

  def toExecutable(compileResults: LoamCompiler.Result): (LoamGraphAstMapping, Executable) = {
    val context: LoamProjectContext = compileResults.contextOpt.get
    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    (mapping, executable)
  }

  def run(executable: Executable): Map[LJob, JobState] = RxExecuter.default.execute(executable)
}