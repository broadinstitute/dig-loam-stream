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
import loamstream.util.Functions
import loamstream.conf.ExecutionConfig
import java.util.concurrent.atomic.AtomicBoolean

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

  def forNamedJob(executionConfig: ExecutionConfig, logger: Loggable, job: LJob): CloseableProcessLogger = {
    import executionConfig.outputDir
    
    toFilesProcessLogger(logger, LogFileNames.stdout(job, outputDir), LogFileNames.stderr(job, outputDir))
  }

  def toFilesProcessLogger(
      loggable: Loggable, 
      stdOutDestination: Path, 
      stdErrDestination: Path): CloseableProcessLogger = {
    
    val writeToStdOut = new LazyWriterRef(loggable, stdOutDestination)
    val writeToStdErr = new LazyWriterRef(loggable, stdErrDestination)
    
    val delegate = ProcessLogger(fout = writeToStdOut.writeFn, ferr = writeToStdErr.writeFn)
    
    CloseableProcessLogger(delegate) {
      writeToStdOut.close()
      writeToStdErr.close()
    }
  }
  
  private[commandline] def makeNeededParentDirs(outputFile: Path): Unit = {
    Option(outputFile.normalize.getParent).foreach(Files.createDirsIfNecessary)
  }
  
  private final class LazyWriterRef(loggable: Loggable, dest: Path) {
    @volatile private[this] var anyWritingDone = false
    
    private[this] lazy val writer: Writer = {
      makeNeededParentDirs(dest)
      
      new BufferedWriter(new FileWriter(dest.toFile))
    }
    
    import Throwables.quietly

    val writeFn: String => Unit = { line =>
      anyWritingDone = true
      
      writer.write(line)
    }
    
    def close(): Unit = if(anyWritingDone) { quietly(s"Error closing Writer for $dest")(writer.close())(loggable) }
  }
}
