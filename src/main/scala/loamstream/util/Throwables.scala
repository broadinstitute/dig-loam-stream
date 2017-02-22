package loamstream.util

import scala.util.control.NonFatal

/**
 * @author clint
 * Dec 7, 2016
 */
object Throwables {
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
