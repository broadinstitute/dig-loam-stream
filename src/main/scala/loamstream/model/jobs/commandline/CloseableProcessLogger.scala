package loamstream.model.jobs.commandline

import scala.sys.process.ProcessLogger
import loamstream.util.Functions

/**
 * @author clint
 * Nov 15, 2017
 */
final case class CloseableProcessLogger(delegate: ProcessLogger)(closeHook: => Any) extends ProcessLogger {
  private[this] val closeFn: () => Any = Functions.memoize(() => closeHook)

  def close(): Unit = closeFn()

  /**
   * Will be called with each line read from the process output stream.
   */
  override def out(s: => String): Unit = delegate.out(s)

  /**
   * Will be called with each line read from the process error stream.
   */
  override def err(s: => String): Unit = delegate.err(s)

  /**
   * If a process is begun with one of these `ProcessBuilder` methods:
   *  {{{
   *    def !(log: ProcessLogger): Int
   *    def !<(log: ProcessLogger): Int
   *  }}}
   *  The run will be wrapped in a call to buffer.  This gives the logger
   *  an opportunity to set up and tear down buffering.  At present the
   *  library implementations of `ProcessLogger` simply execute the body
   *  unbuffered.
   */
  override def buffer[T](f: => T): T = delegate.buffer(f)
}
