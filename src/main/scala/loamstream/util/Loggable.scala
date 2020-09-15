package loamstream.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on: 3/3/16
  * Adapted from https://open.med.harvard.edu/stash/projects/SHRINE/repos/shrine/browse/commons/util/src/main/scala/net/
  * shrine/log/Loggable.scala?at=308b6e8da08c1e41b3ccc7c4c7d2b11155f19c0d&raw
  *
  * @author Kaan Yuksel
  */
trait Loggable extends LogContext {
  private[this] lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  
  import LogContext.Level
  
  protected implicit val logContext: LogContext = this
  
  override final def log(level: Level, s: => String): Unit = level match {
    case Level.Trace => logger.trace(s)
    case Level.Debug => logger.debug(s)
    case Level.Info => logger.info(s)
    case Level.Warn => logger.warn(s)
    case Level.Error => logger.error(s)
  }

  override final def log(level: Level, s: => String, e: Throwable): Unit = level match {
    case Level.Trace => logger.trace(s, e)
    case Level.Debug => logger.debug(s, e)
    case Level.Info => logger.info(s, e)
    case Level.Warn => logger.warn(s, e)
    case Level.Error => logger.error(s, e)
  }

  final override def isTraceEnabled: Boolean = logger.isTraceEnabled
  final override def isDebugEnabled: Boolean = logger.isDebugEnabled
  final override def isInfoEnabled: Boolean = logger.isInfoEnabled
  final override def isWarnEnabled: Boolean = logger.isWarnEnabled
  final override def isErrorEnabled: Boolean = logger.isErrorEnabled
}
