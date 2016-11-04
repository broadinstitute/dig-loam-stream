package loamstream.apps

import loamstream.cli.Conf
import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.Loggable

/**
 * @author kyuksel
 * date: Jul 5, 2016
 *
 * Meant to compile user-specified Loam scripts on the command line for development purposes
 */
object CompileLoamFileApp extends App with Loggable {
  val cli = Conf(args)
  val conf = cli.conf()
  val loams = cli.loam()

  val loamEngine = LoamEngine.default(LoggableOutMessageSink(this))
  val compilationResultShot =  loamEngine.compileFiles(loams)
  assert(compilationResultShot.nonEmpty, compilationResultShot.message)

  val compilationResult = compilationResultShot.get
  info(compilationResult.report)
}
