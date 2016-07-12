package loamstream.apps

import loamstream.compiler.LoamEngine
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.Loggable

/**
  * Created by kyuksel on 7/12/16.
  */
object CompileLoamFileApp extends App with Loggable {
  if (args.length < 1) {
    throw new IllegalArgumentException("No Loam script file name provided")
  }

  if (args.length > 1) {
    throw new IllegalArgumentException("This app takes only one argument, the Loam script file name.")
  }

  val loamFile = args(0)
  val loamEngine = LoamEngine.default(LoggableOutMessageSink(this))
  val compilationResultShot =  loamEngine.compileFile(loamFile)
  assert(compilationResultShot.nonEmpty, compilationResultShot.message)

  val compilationResult = compilationResultShot.get
  info(compilationResult.report)
}
