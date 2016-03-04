package utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created on: 3/3/16
 * Adapted from https://open.med.harvard.edu/stash/projects/SHRINE/repos/shrine/browse/commons/util/src/main/scala/net/
 * shrine/log/Loggable.scala?at=308b6e8da08c1e41b3ccc7c4c7d2b11155f19c0d&raw
 * @author Kaan Yuksel 
 */
trait Loggable {
  private[this] lazy val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  final def trace(s: => String): Unit = if(logger.isTraceEnabled) logger.trace(s)
  final def trace(s: => String, e: Throwable): Unit = if(logger.isTraceEnabled) logger.trace(s, e)

  final def debug(s: => String): Unit = if(logger.isDebugEnabled) logger.debug(s)
  final def debug(s: => String, e: Throwable): Unit = if(logger.isDebugEnabled) logger.debug(s, e)

  final def info(s: => String): Unit = if(logger.isInfoEnabled) logger.info(s)
  final def info(s: => String, e: Throwable): Unit = if(logger.isInfoEnabled) logger.info(s, e)

  final def warn(s: => String): Unit = if(logger.isWarnEnabled) logger.warn(s)
  final def warn(s: => String, e: Throwable): Unit = if(logger.isWarnEnabled) logger.warn(s, e)

  final def error(s: => String): Unit = if(logger.isErrorEnabled) logger.error(s)
  final def error(s: => String, e: Throwable): Unit = if(logger.isErrorEnabled) logger.error(s, e)
}
