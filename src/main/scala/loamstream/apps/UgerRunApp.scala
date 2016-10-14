package loamstream.apps

import com.typesafe.config.ConfigFactory
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter
import loamstream.uger.UgerChunkRunner
import loamstream.util.Loggable
import loamstream.model.execute.JobFilter

/** Compiles and runs Loam script provided as argument */
object UgerRunApp extends App with DrmaaClientHelpers with Loggable {
  if (args.length < 1) {
    throw new IllegalArgumentException("No Loam script file name provided")
  }

  val ugerConfig = UgerConfig.fromConfig(ConfigFactory.load("loamstream.conf")).get

  info("Creating reactive executer...")

  withClient { drmaaClient =>
    import loamstream.model.execute.ExecuterHelpers._
    val size = 50
    val executionContextWithThreadPool = threadPool(size)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    //TODO: This should be configurable
    val pollingFrequencyInHz = 0.017
    val pollingPeriod = (1 / pollingFrequencyInHz).seconds
    
    val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient, pollingFrequencyInHz)

    val jobFilter = JobFilter.RunEverything //TODO
    
    val executer = RxExecuter(chunkRunner, pollingPeriod, jobFilter)(executionContextWithThreadPool)

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
