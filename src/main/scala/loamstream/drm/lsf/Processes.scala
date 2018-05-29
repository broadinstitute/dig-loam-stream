package loamstream.drm.lsf

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
 * @author clint
 * May 17, 2018
 */
object Processes {
  import scala.sys.process._
  
  def runSync(executable: String, processBuilder: ProcessBuilder): Try[RunResults] = {
    val stdOutBuffer: Buffer[String] = new ArrayBuffer
    val stdErrBuffer: Buffer[String] = new ArrayBuffer
    
    val processLogger = ProcessLogger(stdOutBuffer += _, stdErrBuffer += _)
    
    Try {
      val exitCode = processBuilder.!(processLogger)
    
      RunResults(executable, exitCode, stdOutBuffer.toList, stdErrBuffer.toList)
    }
  }
}
