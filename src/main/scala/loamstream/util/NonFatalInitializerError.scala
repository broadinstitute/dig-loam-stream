package loamstream.util

import scala.util.control.NonFatal

/** Matches NonFatal or ExceptionInInitializer with NonFatal cause */
object NonFatalInitializer {

  def fold[T](throwable: Throwable, nonFatal: => T, initNonFatal: => T, other: => T): T = throwable match {
    case NonFatal(nonFatalThrowable) => nonFatal
    case exInitError: ExceptionInInitializerError => exInitError.getCause match {
      case NonFatal(cause) => initNonFatal
      case _ => other
    }
    case _ => other
  }

  /** True if NonFatal or ExceptionInInitializer with NonFatal cause */
  def apply(throwable: Throwable): Boolean = fold[Boolean](throwable, true, true, false)

  /** Some if NonFatal or ExceptionInInitializer with NonFatal cause */
  def unapply(throwable: Throwable): Option[Throwable] = {
    fold[Option[Throwable]](throwable, Some(throwable), Some(throwable.getCause), None)
  }

}
