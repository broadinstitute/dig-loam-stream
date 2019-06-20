package loamstream.util

/**
 * @author clint
 * May 15, 2018
 */
sealed trait RunResults {
  
  def commandLine: String
  
  def stdout: Seq[String]
  
  def stderr: Seq[String]
  
  def exitCode: Int

  final def logStdOutAndStdErr(
      headerMessage: String, 
      level: Loggable.Level = Loggable.Level.Error)(implicit logCtx: LogContext): Unit = {
    
    def doLog(s: => String): Unit = logCtx.log(level, s)
    
    doLog(headerMessage)
    stderr.foreach(line => doLog(s"'${commandLine}' <via stderr>: $line"))
    stdout.foreach(line => doLog(s"'${commandLine}' <via stdout>: $line"))
  }
}

object RunResults {
  def apply(executable: String, exitCode: Int, stdout: Seq[String], stderr: Seq[String]): RunResults = {
    if(ExitCodes.isSuccess(exitCode)) { Successful(executable, stdout, stderr) }
    else { Unsuccessful(executable, exitCode, stdout, stderr) }
  }
  
  final case class Successful(commandLine: String, stdout: Seq[String], stderr: Seq[String]) extends RunResults {
    override def exitCode: Int = 0
  }
  
  final case class Unsuccessful(
      commandLine: String, 
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String]) extends RunResults
}
