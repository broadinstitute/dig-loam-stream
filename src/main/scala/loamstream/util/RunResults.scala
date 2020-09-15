package loamstream.util

import scala.util.Failure
import scala.util.Try
import scala.util.Success

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
      level: LogContext.Level = LogContext.Level.Error)(implicit logCtx: LogContext): Unit = {
    
    def doLog(s: => String): Unit = logCtx.log(level, s)
    
    doLog(headerMessage)
    stderr.foreach(line => doLog(s"'${commandLine}' <via stderr>: $line"))
    stdout.foreach(line => doLog(s"'${commandLine}' <via stdout>: $line"))
  }
  
  final def tryAsSuccess(implicit ctx: LogContext): Try[RunResults.Successful] = tryAsSuccess("")
  
  final def tryAsSuccess(extraMessage: String)(implicit ctx: LogContext): Try[RunResults.Successful] = this match {
    //Coerce invocations producing non-zero exit codes to Failures
    case r: RunResults.Unsuccessful => {
      val msg = s"Error invoking '${r.commandLine}' (exit code ${r.exitCode}): $extraMessage"

      r.logStdOutAndStdErr(s"$msg; output streams follow:", LogContext.Level.Warn)

      Tries.failure(msg)
    }
    case r: RunResults.CouldNotStart => {
      val msg = s"Couldn't run '${r.commandLine}': $extraMessage: No exit code, caught "

      ctx.warn(msg, r.cause)
      
      r.logStdOutAndStdErr(s"$msg; output streams follow:", LogContext.Level.Warn)
      
      Failure(r.cause)
    }
    case r: RunResults.Successful => Success(r)
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
