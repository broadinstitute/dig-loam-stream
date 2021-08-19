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
  
  // final def tryAsSuccess(implicit ctx: LogContext): Try[RunResults.Completed] = {
  //   tryAsSuccess("", RunResults.SuccessPredicate.zeroIsSuccess)
  // }
  
  final def tryAsSuccess(
    extraMessage: String,
    isSuccess: RunResults.SuccessPredicate)(implicit ctx: LogContext): Try[RunResults.Completed] = this match {

    //Coerce invocations producing non-zero exit codes to Failures
    case r: RunResults.Completed if isSuccess(r) => Success(r) 
    case r: RunResults.Completed => {
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
  }
}

object RunResults {
  trait SuccessPredicate extends (RunResults => Boolean)

  object SuccessPredicate {
    def apply(p: RunResults => Boolean): SuccessPredicate = r => p(r)

    implicit final class SuccessPredicateOps(val sp: SuccessPredicate) extends AnyVal {
      def &&(other: SuccessPredicate): SuccessPredicate = r => sp(r) && other(r)

      def ||(other: SuccessPredicate): SuccessPredicate = r => sp(r) || other(r)
    }

    val zeroIsSuccess: SuccessPredicate = ByExitCode.zeroIsSuccess

    object ByExitCode {
      val zeroIsSuccess: SuccessPredicate = countsAsSuccess(ExitCodes.isSuccess(_))

      def countsAsSuccess(p: Int => Boolean): SuccessPredicate = {
        case Completed(_, exitCode, _, _) => p(exitCode)
        case _ => false
      }
      
      def countsAsSuccess(exitCode: Int, others: Int*): SuccessPredicate = countsAsSuccess((exitCode +: others).toSet)
    }
  }

  def apply(
      executable: String, 
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String]): RunResults = Completed(executable, exitCode, stdout, stderr)
  
  final case class Completed(
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
