package loamstream.util

/**
 * @author clint
 * Dec 7, 2016
 */
trait LogContext {
  import LogContext.Level
  
  def log(level: Level, s: => String): Unit
  
  def log(level: Level, s: => String, e: Throwable): Unit 
  
  def isTraceEnabled: Boolean
  def isDebugEnabled: Boolean
  def isInfoEnabled: Boolean
  def isWarnEnabled: Boolean
  def isErrorEnabled: Boolean
  
  final def trace(s: => String): Unit = if (isTraceEnabled) { log(Level.Trace, s) }

  final def trace(s: => String, e: Throwable): Unit = if (isTraceEnabled) { log(Level.Trace, s, e) }

  final def debug(s: => String): Unit = if (isDebugEnabled) { log(Level.Debug, s) }

  final def debug(s: => String, e: Throwable): Unit = if (isDebugEnabled) { log(Level.Debug, s, e) }

  final def info(s: => String): Unit = if (isInfoEnabled) { log(Level.Info, s) }

  final def info(s: => String, e: Throwable): Unit = if (isInfoEnabled) { log(Level.Info, s, e) }

  final def warn(s: => String): Unit = if (isWarnEnabled) { log(Level.Warn, s) }

  final def warn(s: => String, e: Throwable): Unit = if (isWarnEnabled) { log(Level.Warn, s, e) }

  final def error(s: => String): Unit = if (isErrorEnabled) { log(Level.Error, s) }

  final def error(s: => String, e: Throwable): Unit = if (isErrorEnabled) { log(Level.Error, s, e) }
}

object LogContext {
  trait AllLevelsDisabled { self: LogContext =>
    override def isTraceEnabled: Boolean = false
    override def isDebugEnabled: Boolean = false
    override def isInfoEnabled: Boolean = false
    override def isWarnEnabled: Boolean = false
    override def isErrorEnabled: Boolean = false
  }
  
  trait AllLevelsEnabled { self: LogContext =>
    override def isTraceEnabled: Boolean = true
    override def isDebugEnabled: Boolean = true
    override def isInfoEnabled: Boolean = true
    override def isWarnEnabled: Boolean = true
    override def isErrorEnabled: Boolean = true
  }
  
  object Noop extends LogContext with AllLevelsDisabled {
    override def log(level: Level, s: => String): Unit = ()
  
    override def log(level: Level, s: => String, e: Throwable): Unit = () 
  }
  
  sealed abstract class Level private[Level] (private val index: Int) {
    final def >=(that: Level): Boolean = this.index >= that.index
    
    final def name: String = toString
  }
  
  object Level {
    //scalastyle:off magic.number
    case object Trace extends Level(0)
    case object Debug extends Level(1)
    case object Info extends Level(2)
    case object Warn extends Level(3)
    case object Error extends Level(4)
    //scalastyle:on magic.number
  }
  
  object Implicits {
    implicit val Noop = LogContext.Noop
  }
}
