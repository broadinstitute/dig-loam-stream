package loamstream.loam.intake

import java.nio.file.Path
import loamstream.util.LogContext
import loamstream.util.LogContext.Level
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.LocalDateTime
import java.io.PrintWriter
import scala.collection.Seq
import loamstream.util.IoUtils

/**
 * @author clint
 * Oct 22, 2020
 */
final class ToFileLogContext(
    path: Path, 
    append: Boolean = false) extends LogContext with LogContext.AllLevelsEnabled with Closeable {
  
  private lazy val writer: PrintWriter = {
    val opts: Seq[StandardOpenOption] = {
      StandardOpenOption.CREATE +: (if(append) Seq(StandardOpenOption.APPEND) else Nil)
    }
    
    new PrintWriter(Files.newBufferedWriter(path, opts: _*))
  }
  
  override def close(): Unit = writer.close()
  
  override def log(level: Level, s: => String): Unit = doLog(level, s, None)
  
  override def log(level: Level, s: => String, e: Throwable): Unit = doLog(level, s, Option(e))
  
  private def doLog(level: Level, s: => String, eOpt: Option[Throwable]): Unit = {
    def prefix = s"[${level}][${LocalDateTime.now}]"
    
    IoUtils.printTo(writer)(s"${prefix} ${s}")
    
    eOpt.foreach { e =>
      IoUtils.printTo(writer)(s"${prefix} ${e}")
      
      e.printStackTrace(writer)
    }
    
    //TODO: FIXME, it's probably better to call close() once at the end
    //writer.flush()
  }
}
