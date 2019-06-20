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
  
  def runSync(commandLineTokens: Seq[String]): Try[RunResults] = {
    import scala.sys.process._
    
    //NB: Implicit conversion to ProcessBuilder :\ 
    val processBuilder: ProcessBuilder = commandLineTokens
    
    runSync(commandLineTokens.mkString(" "), processBuilder)
  }
  
  def runSync(
      commandLine: String, 
      processBuilder: ProcessBuilder)(implicit logCtx: LogContext): Try[RunResults] = {
    
    val processLogger = ProcessLoggers.buffering
    
    Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(commandLine, exitCode, processLogger.stdOut, processLogger.stdErr)
    }
  }
}
