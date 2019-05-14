package loamstream.util

/**
 * @author clint
 * May 15, 2018
 */
sealed trait RunResults {
  
  def executable: String
  
  def stdout: Seq[String]
  
  def stderr: Seq[String]

  final def logStdOutAndStdErr(
      headerMessage: String, 
      level: Loggable.Level.Value = Loggable.Level.error)(implicit logCtx: LogContext): Unit = {
    
    def doLog(s: => String): Unit = logCtx.log(level, s)
    
    doLog(headerMessage)
    stderr.foreach(line => doLog(s"${executable} <via stderr>: $line"))
    stdout.foreach(line => doLog(s"${executable} <via stdout>: $line"))
  }
}

object RunResults {
  def apply(executable: String, exitCode: Int, stdout: Seq[String], stderr: Seq[String]): RunResults = {
    if(ExitCodes.isSuccess(exitCode)) { Successful(executable, stdout, stderr) }
    else { Unsuccessful(executable, exitCode, stdout, stderr) }
  }
  
  final case class Successful(executable: String, stdout: Seq[String], stderr: Seq[String]) extends RunResults
  
  final case class Unsuccessful(
      executable: String, 
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String]) extends RunResults
}
