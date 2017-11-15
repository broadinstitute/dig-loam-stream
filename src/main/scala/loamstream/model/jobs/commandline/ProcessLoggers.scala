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
import loamstream.util.Files

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

  def toFilesProcessLogger(
      logger: Loggable, 
      stdOutDestination: Path, 
      stdErrDestination: Path): CloseableProcessLogger = {
    
    def makeNeededDirs(outputFile: Path): Unit = {
      Option(outputFile.normalize.getParent).foreach(Files.createDirsIfNecessary)
    }
    
    def writerFor(p: Path): Writer = {
      makeNeededDirs(p)
      
      new BufferedWriter(new FileWriter(p.toFile))
    }
    
    lazy val stdout = writerFor(stdOutDestination)
    lazy val stderr = writerFor(stdErrDestination)
    
    //TODO: Giant hack
    @volatile var wroteToStdOut = false
    @volatile var wroteToStdErr = false
    
    val writeToStdOut: String => Unit = { line =>
      wroteToStdOut = true
      stdout.write(line) 
    }
    
    val writeToStdErr: String => Unit = { line =>
      wroteToStdErr = true
      stderr.write(line) 
    }

    val delegate = ProcessLogger(fout = writeToStdOut, ferr = writeToStdErr)
    
    import Throwables.quietly
    
    def close(path: Path, writer: Writer): Unit = {
      quietly(s"Error closing Writer for $path")(writer.close())(logger)
    }
    
    CloseableProcessLogger(delegate) {
      if(wroteToStdOut) { close(stdOutDestination, stdout) }
      if(wroteToStdErr) { close(stdErrDestination, stderr) }
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
