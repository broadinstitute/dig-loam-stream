package loamstream.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Success

/**
 * @author clint
 * May 17, 2018
 */
object Processes extends Loggable {
  import scala.sys.process._
  
  def runSync(executable: String, commandLineTokens: Seq[String]): Try[RunResults] = {
    import scala.sys.process._
    
    //NB: Implicit conversion to ProcessBuilder :\ 
    val processBuilder: ProcessBuilder = commandLineTokens
    
    runSync(executable, processBuilder)
  }
  
  def runSync(
      executable: String, 
      processBuilder: ProcessBuilder)(implicit logCtx: LogContext): Try[RunResults] = {
    
    val stdOutBuffer: Buffer[String] = new ArrayBuffer
    val stdErrBuffer: Buffer[String] = new ArrayBuffer
    
    val processLogger = ProcessLogger(stdOutBuffer += _, stdErrBuffer += _)
    
    val attempt = Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(executable, exitCode, stdOutBuffer.toList, stdErrBuffer.toList)
    }
    
    attempt match {
      case s @ Success(runResults) if runResults.isFailure => {
        val msg = s"Error invoking ${executable} (exit code ${runResults.exitCode})"
        
        runResults.logStdOutAndStdErr(s"$msg; output streams follow:", Loggable.Level.warn)(logCtx)
        
        Tries.failure(msg)
      }
      case attempt => attempt
    }
  }
}
