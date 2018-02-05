package loamstream.util

import scala.util.control.NonFatal
import scala.util.Try

/**
 * @author clint
 * Dec 7, 2016
 */
object Throwables {
  def collectFailures(blocks: (() => Any)*): Seq[Throwable] = {
    blocks.flatMap(b => failureOption(b()))
  }
  
  def failureOption(f: => Any): Option[Throwable] = Try(f).failed.toOption
  
  def quietly(
      message: String, 
      level: Loggable.Level.Value = Loggable.Level.error)
      (f: => Any)
      (implicit logContext: LogContext): Option[Throwable] = {
    
    try { 
      f 
      
      None
    } catch { 
      case NonFatal(e) => {
        logContext.log(level, message, e)
        
        Some(e)
      }
    }
  }
}
