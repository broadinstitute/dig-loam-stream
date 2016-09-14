package loamstream.model.jobs.commandline

import loamstream.model.jobs.JobState.{Failed, Running, Succeeded}
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.commandline.CommandLineJob.{CommandException, CommandResult}
import loamstream.model.jobs.commandline.CommandLineJob.{CommandReturnValueIssue, CommandSuccess}

import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.{ProcessBuilder, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  */

/** A job based on a command line definition */
trait CommandLineJob extends LJob {
  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger = CommandLineJob.noOpProcessLogger

  def exitValueCheck: Int => Boolean
  
  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  type CommandLineResult = Result with CommandResult

  override def execute(implicit context: ExecutionContext): Future[Result with CommandResult] = {
    runBlocking {
      trace(s"RUNNING: $commandLineString")
      updateAndEmitJobState(Running)

      val exitValue = processBuilder.run(logger).exitValue
  
      if (exitValueIsOk(exitValue)) {
        trace(s"SUCCEEDED: $commandLineString")
        updateAndEmitJobState(Succeeded)
        CommandSuccess(commandLineString, exitValue)
      } else {
        trace(s"FAILED: $commandLineString")
        updateAndEmitJobState(Failed)
        CommandReturnValueIssue(commandLineString, exitValue)
      }
    }.recover {
      case exception: Exception => CommandException(commandLineString, exception)
    }
  }

  override def toString: String = commandLineString
}

object CommandLineJob {

  val mustBeZero: Int => Boolean = _ == 0
  val acceptAll : Int => Boolean = i => true
  
  val defaultExitValueChecker = mustBeZero

  sealed trait CommandResult

  final case class CommandSuccess(commandLine: String, returnValue: Int) extends LJob.Success with CommandResult {
    override def successMessage: String = s"Successfully completed job '$commandLine'."
  }

  final case class CommandException(commandLine: String, exception: Exception)
    extends LJob.Failure with CommandResult {
    override def failureMessage: String = {
      s"Failed with exception '${exception.getMessage}' when trying command line '$commandLine'"
    }
  }

  final case class CommandReturnValueIssue(commandLine: String, returnValue: Int)
    extends LJob.Failure with CommandResult {
    override def failureMessage: String = {
      s"Undesired return value '$returnValue' when trying command line + '$commandLine'"
    }
  }

  val noOpProcessLogger = ProcessLogger(line => ())

}
