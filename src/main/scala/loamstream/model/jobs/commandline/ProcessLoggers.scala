package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import java.nio.file.Path
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.Writer
import loamstream.model.jobs.LJob
import java.nio.file.Paths
import loamstream.util.Loggable
import loamstream.util.Throwables
import loamstream.model.jobs.LogFileNames

/**
 * @author clint
 * Nov 13, 2017
 */
object ProcessLoggers {
  def stdErrProcessLogger(logger: Loggable): ProcessLogger = {
    val noop: String => Unit = _ => ()

    ProcessLogger(
      fout = noop,
      ferr = line => logger.info(s"(via stderr) $line"))
  }

  def forNamedJob(logger: Loggable, job: LJob): CloseableProcessLogger = {
    toFilesProcessLogger(logger, LogFileNames.stdout(job), LogFileNames.stderr(job))
  }

  def toFilesProcessLogger(logger: Loggable, stdOutDestination: Path, stdErrDestination: Path): CloseableProcessLogger = {
    def writerFor(p: Path) = new BufferedWriter(new FileWriter(stdOutDestination.toFile))

    val stdout = writerFor(stdOutDestination)
    val stderr = writerFor(stdErrDestination)

    def writeTo(writer: Writer): String => Unit = writer.write(_: String)

    val delegate = ProcessLogger(fout = writeTo(stdout), ferr = writeTo(stderr))
    
    import Throwables.quietly
    
    def close(path: Path, writer: Writer): Unit = {
      quietly(s"Error closing Writer for $path")(writer.close())(logger)
    }
    
    CloseableProcessLogger(delegate) {
      close(stdOutDestination, stdout)
      close(stdErrDestination, stderr)
    }
  }
  
  final case class CloseableProcessLogger(delegate: ProcessLogger)(closeHook: => Any) extends ProcessLogger {
    def close(): Unit = closeHook

    /**
     * Will be called with each line read from the process output stream.
     */
    override def out(s: => String): Unit = delegate.out(s)

    /**
     * Will be called with each line read from the process error stream.
     */
    override def err(s: => String): Unit = delegate.err(s)

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
    override def buffer[T](f: => T): T = delegate.buffer(f)
  }
}
