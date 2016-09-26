package loamstream.apps

import com.typesafe.config.ConfigFactory
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter
import loamstream.uger.UgerChunkRunner
import loamstream.util.Loggable

/** Compiles and runs Loam script provided as argument */
object RxLoamRunApp extends App with DrmaaClientHelpers with Loggable {
  if (args.length < 1) {
    throw new IllegalArgumentException("No Loam script file name provided")
  }

  val ugerConfig = UgerConfig.fromConfig(ConfigFactory.load("loamstream.conf")).get

  info("Creating reactive executer...")

  withClient { drmaaClient =>
    import loamstream.model.execute.ExecuterHelpers._
    val size = 50
    val executionContextWithThreadPool = threadPool(size)

    val pollingFrequencyInHz = 0.1
    val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient, pollingFrequencyInHz)

    val executer = RxExecuter(chunkRunner)(executionContextWithThreadPool)

    val outMessageSink = LoggableOutMessageSink(this)

    val loamEngine =
      LoamEngine(new LoamCompiler(LoamCompiler.Settings.default, outMessageSink), executer, outMessageSink)
    val engineResult = loamEngine.runFilesWithNames(args)

    for {
      (job, result) <- engineResult.jobResultsOpt.get
    } {
      info(s"Got $result when running $job")
    }
  }
}
