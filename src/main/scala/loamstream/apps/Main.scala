package loamstream.apps

import loamstream.cli.Conf
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import loamstream.uger.DrmaaClient
import rx.lang.scala.Scheduler
import loamstream.uger.JobMonitor
import loamstream.uger.Poller
import loamstream.util.Loggable
import loamstream.conf.UgerConfig
import loamstream.uger.UgerChunkRunner
import loamstream.db.slick.DbType
import loamstream.db.slick.SlickLoamDao
import loamstream.model.execute.RxExecuter
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.db.slick.DbDescriptor
import loamstream.cli.BackendType
import org.ggf.drmaa.DrmaaException

/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    val cli = Conf(args)

    if (cli.compileOnly.isSupplied) {
      compileOnly(cli)
    } else {
      run(cli)
    }
  }
  
  private def outMessageSink = LoggableOutMessageSink(this)

  private def run(cli: Conf): Unit = {
    val wiring = cli.backend() match {
      case BackendType.Local => AppWiring.forLocal(cli)
      case BackendType.Uger  => AppWiring.forUger(cli)
    }

    val loamEngine = {
      val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)

      LoamEngine(loamCompiler, wiring.executer, outMessageSink)
    }

    try {
      val engineResult = loamEngine.runFiles(cli.loams())

      for {
        (job, result) <- engineResult.jobResultsOpt.get
      } {
        info(s"Got $result when running $job")
      }
    } catch {
      case e: DrmaaException => warn(s"Unexpected DRMAA exception: ${e.getClass.getName}", e)
    } finally {
      wiring.shutdown()
    }
  }

  private def compileOnly(cli: Conf): Unit = {
    val loamEngine = LoamEngine.default(LoggableOutMessageSink(this))

    val compilationResultShot = loamEngine.compileFiles(cli.loams())

    assert(compilationResultShot.nonEmpty, compilationResultShot.message)

    val compilationResult = compilationResultShot.get

    info(compilationResult.report)
  }
}