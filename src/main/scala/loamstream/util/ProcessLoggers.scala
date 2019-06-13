package loamstream.util

import java.nio.file.Path
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.sys.process.ProcessLogger


/**
 * @author clint
 * Nov 13, 2017
 */
object ProcessLoggers {
  def toFilesInDir(jobOutputDir: Path): ToFilesProcessLogger = {
    ToFilesProcessLogger(LogFileNames.outputStreams(jobOutputDir))
  }
  
  def buffering: ProcessLoggers.Buffering = new ProcessLoggers.Buffering()

  final class Buffering(
      stdOutBuffer: Buffer[String] = new ArrayBuffer,
      stdErrBuffer: Buffer[String] = new ArrayBuffer) extends ProcessLogger {
    
    def stdOut: Seq[String] = stdOutBuffer.toList
    def stdErr: Seq[String] = stdErrBuffer.toList
    
    //Methods from ProcessLogger 
    override def out(s: => String): Unit = stdOutBuffer += s

    override def err(s: => String): Unit = stdErrBuffer += s

    /**
     * If a process is begun with one of these `ProcessBuilder` methods:
     *  {{{
     *    def !(log: ProcessLogger): Int
     *    def !<(log: ProcessLogger): Int
     *  }}}
     *  The run will be wrapped in a call to buffer.  This gives the logger
     *  an opportunity to set up and tear down buffering.  At present the
     *  library implementations of `ProcessLogger` simply execute the body
     *  unbuffered.
     */
    override def buffer[T](f: => T): T = f
  }
}
