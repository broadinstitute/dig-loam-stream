package loamstream.util

/**
 * @author clint
 * May 15, 2018
 */
final case class RunResults(executable: String, exitCode: Int, stdout: Seq[String], stderr: Seq[String]) {
  
  def isSuccess: Boolean = ExitCodes.isSuccess(exitCode)
  
  def isFailure: Boolean = ExitCodes.isFailure(exitCode)
  
  def logStdOutAndStdErr(
      headerMessage: String, 
      level: Loggable.Level.Value = Loggable.Level.error)(implicit logCtx: LogContext): Unit = {
    
    def doLog(s: => String): Unit = logCtx.log(level, s)
    
    doLog(headerMessage)
    stderr.foreach(line => doLog(s"${executable} <via stderr>: $line"))
    stdout.foreach(line => doLog(s"${executable} <via stdout>: $line"))
  }
}
