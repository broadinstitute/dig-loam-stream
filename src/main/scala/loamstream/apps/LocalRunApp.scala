package loamstream.apps

import loamstream.cli.Conf
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.model.execute.RxExecuter
import loamstream.util.Loggable

/**
 * @author kyuksel
 * date: Jul 5, 2016
 *
 * Meant to compile and run (locally) the Loam script provided as argument
 */
object LocalRunApp extends App with Loggable {
  val cli = Conf(args)
  val conf = cli.conf()
  val loams = cli.loam()

  val executer = RxExecuter.default

  val outMessageSink = LoggableOutMessageSink(this)

  val loamEngine = {
    val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)

    LoamEngine(loamCompiler, executer, outMessageSink)
  }

  loamEngine.runFiles(loams)
}
