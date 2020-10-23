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
    
    writer.println(s"${prefix} ${s}")
    
    eOpt.foreach { e =>
      writer.println(s"${prefix} ${e}")
      
      e.printStackTrace(writer)
    }
  }
}
