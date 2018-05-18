package loamstream.drm.lsf

import loamstream.util.ExitCodes
import loamstream.util.LogContext
import loamstream.util.Loggable

/**
 * @author clint
 * May 15, 2018
 */
final case class RunResults(executable: String, exitCode: Int, stdout: Seq[String], stderr: Seq[String]) {
  
  def isSuccess: Boolean = ExitCodes.isSuccess(exitCode)
  
  def isFailure: Boolean = ExitCodes.isFailure(exitCode)
  
  def logStdOutAndStdErr(headerMessage: String)(implicit logCtx: LogContext): Unit = {
    def error(s: => String): Unit = logCtx.log(Loggable.Level.error, s)
    
    error(headerMessage)
    stderr.foreach(line => error(s"${executable} <via stderr>: $line"))
    stdout.foreach(line => error(s"${executable} <via stdout>: $line"))
  }
}
