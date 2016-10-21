package loamstream.apps

import com.typesafe.config.ConfigFactory
import loamstream.cli.Conf
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{ LoamCompiler, LoamEngine }
import loamstream.conf.UgerConfig
import loamstream.model.execute.RxExecuter
import loamstream.uger.UgerChunkRunner
import loamstream.util.Loggable
import com.typesafe.config.Config
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.DbType
import loamstream.util.RxSchedulers
import loamstream.uger.JobMonitor
import rx.lang.scala.Scheduler
import loamstream.uger.DrmaaClient
import loamstream.uger.Poller
import scala.concurrent.ExecutionContext

/** Compiles and runs Loam script provided as argument */
object UgerRunApp extends App with DrmaaClientHelpers with TypesafeConfigHelpers with SchedulerHelpers with Loggable {
  val cli = Conf(args)
  val conf = cli.conf()
  val loams = cli.loam()

  val ugerConfig = UgerConfig.fromConfig(typesafeConfig(conf)).get

  info("Creating reactive executer...")

  withClient { drmaaClient =>
    import loamstream.model.execute.ExecuterHelpers._
    val threadPoolSize = 50
    val executionContextWithThreadPool = threadPool(threadPoolSize)

    val pollingFrequencyInHz = 0.1

    val engineResult = withThreadPoolScheduler(threadPoolSize) { scheduler =>

      withJobMonitor(drmaaClient, scheduler, pollingFrequencyInHz) { jobMonitor =>
        val chunkRunner = UgerChunkRunner(ugerConfig, drmaaClient, jobMonitor, pollingFrequencyInHz)

        val dbDescriptor = DbDescriptor(DbType.H2, "jdbc:h2:.loamstream/db")
  
        val dao = new SlickLoamDao(dbDescriptor)
  
        val executer = RxExecuter(chunkRunner)(executionContextWithThreadPool)
  
        val outMessageSink = LoggableOutMessageSink(this)
  
        val loamEngine = {
          val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)
  
          LoamEngine(loamCompiler, executer, outMessageSink)
        }
  
        loamEngine.runFiles(loams)
      }
    }

    for {
      (job, result) <- engineResult.jobResultsOpt.get
    } {
      info(s"Got $result when running $job")
    }
  }
  
  private def withJobMonitor[A](
      drmaaClient: DrmaaClient, 
      scheduler: Scheduler,
      pollingFrequencyInHz: Double)(f: JobMonitor => A): A = {
    
    val poller = Poller.drmaa(drmaaClient)
    
    val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)
    
    try { f(jobMonitor) }
    finally { jobMonitor.stop() }
  }
}
