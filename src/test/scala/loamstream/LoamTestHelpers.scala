package loamstream

import java.nio.file.{Files, Path, Paths}

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.ast.{LoamGraphAstMapper, LoamGraphAstMapping}
import loamstream.loam.{LoamContext, LoamToolBox}
import loamstream.model.execute.{RxExecuter, LExecutable}
import loamstream.model.jobs.LJob
import loamstream.util.{Loggable, Shot, StringUtils}

import scala.concurrent.ExecutionContext

/**
 * @author clint
 * date: Jul 8, 2016
 */
trait LoamTestHelpers extends Loggable {
  
  def compileFile(file: String)(implicit context: ExecutionContext): LoamCompiler.Result = compile(Paths.get(file))
  
  def compile(path: Path)(implicit context: ExecutionContext): LoamCompiler.Result = {
    val source = StringUtils.fromUtf8Bytes(Files.readAllBytes(path))
    
    compile(source)
  }
  
  def compile(source: String)(implicit context: ExecutionContext): LoamCompiler.Result = {
    
    val compiler = new LoamCompiler(LoggableOutMessageSink(this))
    
    val compileResults = compiler.compile(source)

    if (!compileResults.isValid) {
      throw new IllegalArgumentException(s"Could not compile '$source': ${compileResults.errors}.")
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
  
  def run(executable: LExecutable): Map[LJob, Shot[LJob.Result]] = RxExecuter.default.execute(executable)
}