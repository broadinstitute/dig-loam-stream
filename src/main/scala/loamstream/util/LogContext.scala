package loamstream.util

/**
 * @author clint
 * Dec 7, 2016
 */
trait LogContext {
  def log(level: Loggable.Level, s: => String): Unit
  
  def log(level: Loggable.Level, s: => String, e: Throwable): Unit 
}

object LogContext {
  object Noop extends LogContext {
    override def log(level: Loggable.Level, s: => String): Unit = ()
  
    override def log(level: Loggable.Level, s: => String, e: Throwable): Unit = () 
  }
}
