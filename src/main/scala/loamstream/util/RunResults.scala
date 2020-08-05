package loamstream.util

import scala.util.Failure

/**
 * @author clint
 * May 15, 2018
 */
sealed trait RunResults {
  
  def commandLine: String
  
  def stdout: Seq[String]
  
  def stderr: Seq[String]

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
  def apply(
      executable: String, 
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String],
      isSuccess: Int => Boolean = ExitCodes.isSuccess): RunResults = {
    
    if(isSuccess(exitCode)) { Successful(executable, stdout, stderr) }
    else { Unsuccessful(executable, exitCode, stdout, stderr) }
  }
  
  final case class Successful(commandLine: String, stdout: Seq[String], stderr: Seq[String]) extends RunResults
  
  final case class Unsuccessful(
      commandLine: String, 
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String]) extends RunResults
      
  final case class CouldNotStart(
      commandLine: String,
      cause: Throwable) extends RunResults {
    
    override def stdout: Seq[String] = Nil
  
    override def stderr: Seq[String] = Nil
    
    def toFailure[A]: Failure[A] = Failure(cause)
  }
}
