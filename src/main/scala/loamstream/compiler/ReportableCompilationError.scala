package loamstream.compiler

import scala.util.control.NonFatal

/**
 * @author clint
 * Sep 25, 2017
 */
object ReportableCompilationError {
  /**
   * Matches non-fatal errors, or (otherwise-fatal) ExceptionInInitializerErrors with non-fatal exception fields.
   */
  def unapply(e: Throwable): Option[Throwable] = e match {
    case e: ExceptionInInitializerError if NonFatal(e.getException) => Some(e.getException)
    case NonFatal(e) => Some(e)
    case _ => None
  }
}
