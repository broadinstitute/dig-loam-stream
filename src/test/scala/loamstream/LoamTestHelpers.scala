package loamstream

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

import scala.concurrent.ExecutionContext

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.ChunkedExecuter
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import loamstream.util.Shot

/**
 * @author clint
 * date: Jul 8, 2016
 */
trait LoamTestHelpers extends Loggable {
  
  def compile(path: Path)(implicit context: ExecutionContext): LExecutable = {
    val source = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    
    compile(source)
  }
  
  def compile(source: String)(implicit context: ExecutionContext): LExecutable = {
    val compiler = new LoamCompiler(LoggableOutMessageSink(this))

    val compileResults = compiler.compile(source)

    if (!compileResults.isValid) {
      throw new IllegalArgumentException(s"Could not compile '$source'.")
    }

    val env = compileResults.envOpt.get

    val graph = compileResults.graphOpt.get.withEnv(env)

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = LoamToolBox(env)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)
  
    executable
  }
  
  def run(executable: LExecutable): Map[LJob, Shot[LJob.Result]] = ChunkedExecuter.default.execute(executable)
}