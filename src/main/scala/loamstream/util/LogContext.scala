package loamstream.util

/**
 * @author clint
 * Dec 7, 2016
 */
trait LogContext {
  def log(level: Loggable.Level.Value, s: => String, e: Throwable): Unit 
}