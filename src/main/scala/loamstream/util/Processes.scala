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
    
    Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(executable, exitCode, stdOutBuffer.toList, stdErrBuffer.toList)
    }
  }
}
