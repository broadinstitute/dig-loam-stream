package loamstream.apps

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files => JFiles}

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.Loggable

import scala.concurrent.ExecutionContext.Implicits.global

/** Compiles and runs Loam script provided as argument */
object LoamRunApp extends App with Loggable {
  if (args.length < 2) throw new IllegalArgumentException("No Loam script file name provided")
  if (args.length > 2) {
    throw new IllegalArgumentException("This app takes only one argument, the Loam script file name.")
  }
  val sourcePath = Paths.get(args(1))
  val source = new String(JFiles.readAllBytes(sourcePath), StandardCharsets.UTF_8)
  val compiler = new LoamCompiler(LoggableOutMessageSink(this))(global)
  info(s"Now compiling $sourcePath.")
  val compileResults = compiler.compile(source)
  if (!compileResults.isValid) throw new IllegalArgumentException(s"Could not compile $sourcePath.")
  info(compileResults.summary)
  val env = compileResults.envOpt.get
  val graph = compileResults.graphOpt.get.withEnv(env)
  val mapping = LoamGraphAstMapper.newMapping(graph)
  val toolBox = LoamToolBox(env)
  val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)
  info("Now going to execute.")
  val jobResults = ChunkedExecuter.default.execute(executable)
  info("Done!")
}
