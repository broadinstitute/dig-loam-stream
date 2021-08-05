package loamstream.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import scala.util.control.NonFatal

/**
 * @author clint
 * May 17, 2018
 */
object Processes extends Loggable {
  import scala.sys.process._
  
  def runSync(
      tokens: Seq[String])( 
      //NB: Implicit conversion from Seq[String] => ProcessBuilder :\
      processBuilder: ProcessBuilder = tokens)(implicit logCtx: LogContext): RunResults = {
    
    val processLogger = ProcessLoggers.buffering
    
    def commandLine = tokens.mkString(" ")
    
    try {
      val process = processBuilder.run(processLogger)
      
      val exitCode = try { process.exitValue } finally { process.destroy() }
      
      RunResults(commandLine, exitCode, processLogger.stdOut, processLogger.stdErr)
    } catch {
      case NonFatal(e) => RunResults.CouldNotStart(commandLine, e)
    }
  }
}
