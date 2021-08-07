package loamstream.util

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.io.InputStream

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
      val process = processBuilder.run(processLogger)

      val exitCode = try { process.exitValue } finally { process.destroy() }
      
      RunResults(commandLine, exitCode, processLogger.stdOut, processLogger.stdErr, isSuccess)
    }
  }

  def runAndExposeStreams(tokens: Seq[String])(isSuccess: Int => Boolean = ExitCodes.isSuccess)
     (implicit logCtx: LogContext): Try[(InputStream, InputStream, Terminable)] = {
    Try {
      val processLogger = ProcessLoggers.buffering
      
      val jProcessBuilder = new java.lang.ProcessBuilder().command(tokens: _*)

      val process = jProcessBuilder.start()

      val stdout = process.getInputStream
      val stderr = process.getErrorStream

      val handle = Terminable {
        process.destroy()
      }

      import Terminable._

      (stdout, stderr, Terminable.StopsComponents(handle, stdout.asTerminable, stderr.asTerminable))
    }
  }
}
