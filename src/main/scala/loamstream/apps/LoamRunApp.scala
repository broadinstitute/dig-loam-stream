package loamstream.apps

import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.Loggable

/** Compiles and runs Loam script provided as argument */
object LoamRunApp extends App with Loggable {
  if (args.length < 2) throw new IllegalArgumentException("No Loam script file name provided")
  if (args.length > 2) {
    throw new IllegalArgumentException("This app takes only one argument, the Loam script file name.")
  }
  val loamEngine = LoamEngine.default(LoggableOutMessageSink(this))
  loamEngine.runFile(args(1))
}
