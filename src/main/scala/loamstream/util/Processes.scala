package loamstream.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * May 17, 2018
 */
object Processes extends Loggable {
  import scala.sys.process._
  
  def runSync(
      tokens: Seq[String])( 
      //NB: Implicit conversion from Seq[String] => ProcessBuilder :\
      processBuilder: ProcessBuilder = tokens,
      isSuccess: Int => Boolean = ExitCodes.isSuccess)(implicit logCtx: LogContext): Try[RunResults] = {
    
    val processLogger = ProcessLoggers.buffering
    
    def commandLine = tokens.mkString(" ")
    
    Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(commandLine, exitCode, processLogger.stdOut, processLogger.stdErr, isSuccess)
    }
  }
}
