package loamstream.apps

import java.nio.charset.StandardCharsets
import java.nio.file.{ Paths, Files => JFiles }

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.Loggable

import scala.concurrent.ExecutionContext.Implicits.global
import loamstream.uger.DrmaaClient
import loamstream.uger.Drmaa1Client
import loamstream.uger.UgerChunkRunner
import loamstream.conf.UgerConfig
import loamstream.model.execute.LExecutable

/** Compiles and runs Loam script provided as argument */
object LoamRunApp extends App with DrmaaClientHelpers with Loggable {
  if (args.length < 1) {
    throw new IllegalArgumentException("No Loam script file name provided")
  }

  if (args.length > 1) {
    throw new IllegalArgumentException("This app takes only one argument, the Loam script file name.")
  }

  val sourcePath = Paths.get(args.head)

  val source = new String(JFiles.readAllBytes(sourcePath), StandardCharsets.UTF_8)

  val compiler = new LoamCompiler(LoggableOutMessageSink(this))(global)

  info(s"Now compiling $sourcePath.")

  val compileResults = compiler.compile(source)

  if (!compileResults.isValid) {
    throw new IllegalArgumentException(s"Could not compile $sourcePath.")
  }

  info(compileResults.summary)

  val env = compileResults.envOpt.get

  val graph = compileResults.graphOpt.get.withEnv(env)

  val mapping = LoamGraphAstMapper.newMapping(graph)

  val toolBox = LoamToolBox(env)

  val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

  val ugerConfig = UgerConfig.fromFile("loamstream.conf").get

  info("Making Executer")

  withClient { drmaaClient =>
    import scala.concurrent.ExecutionContext.Implicits.global

    val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient)

    val executer = ChunkedExecuter(chunkRunner)

    info(s"Executing...")

    val results = executer.execute(executable)

    info(s"Run complete; results:")

    for {
      (job, result) <- results
    } {
      info(s"Got $result when running $job")
    }
  }

  info("Done!")
}
