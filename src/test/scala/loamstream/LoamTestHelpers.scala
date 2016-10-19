package loamstream

import java.nio.file.{ Files, Path, Paths }

import scala.concurrent.ExecutionContext

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.{ LoamContext, LoamToolBox }
import loamstream.loam.ast.{ LoamGraphAstMapper, LoamGraphAstMapping }
import loamstream.model.execute.Executable
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.LJob
import loamstream.util.{ Loggable, Shot, StringUtils }
import loamstream.model.jobs.JobState
import loamstream.loam.LoamScript

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
    val context: LoamContext = compileResults.contextOpt.get
    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    (mapping, executable)
  }

  def run(executable: Executable): Map[LJob, JobState] = RxExecuter.default.execute(executable)
}