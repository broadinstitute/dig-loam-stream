package loamstream.apps

import loamstream.cli.Conf
import loamstream.util.Loggable
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.cli.BackendType
import org.ggf.drmaa.DrmaaException

/**
 * @author clint
 * Nov 10, 2016
 */
object Main extends Loggable {
  def main(args: Array[String]): Unit = {
    val cli = Conf(args)

    if (cli.dryRun.isSupplied) {
      compileOnly(cli)
    } else {
      run(cli)
    }
  }
  
  private def outMessageSink = LoggableOutMessageSink(this)

  private def run(cli: Conf): Unit = {
    val wiring = AppWiring(cli)

    val loamEngine = {
      val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)

      LoamEngine(loamCompiler, wiring.executer, outMessageSink, wiring.cloudStorageClient)
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
      shutdown(wiring)
    }
  }
  
  private def shutdown(wiring: AppWiring): Unit = {
    wiring.shutdown() match {
      case Nil => info("LoamStream shut down successfully")
      case exceptions => {
        error(s"LoamStream shut down with ${exceptions.size} errors: ")

        exceptions.foreach { e =>
          error(s"Error shuting down: ${e.getClass.getName}", e)
        }
      }
    }
  }

  private def compileOnly(cli: Conf): Unit = {
    val loamEngine = LoamEngine.default(outMessageSink)

    val compilationResultShot = loamEngine.compileFiles(cli.loams())

    assert(compilationResultShot.nonEmpty, compilationResultShot.message)

    val compilationResult = compilationResultShot.get

    info(compilationResult.report)
  }
}
