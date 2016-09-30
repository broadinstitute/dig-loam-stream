package loamstream.model.jobs.commandline

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.commandline.CommandLineJob.{CommandException, CommandResult}
import loamstream.model.jobs.commandline.CommandLineJob.{CommandReturnValueIssue, CommandSuccess}

import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.{ProcessBuilder, ProcessLogger}
import loamstream.model.jobs.JobState
import loamstream.util.Futures

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

  override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = {
    Futures.runBlocking {
      trace(s"RUNNING: $commandLineString")
      val exitValue = processBuilder.run(logger).exitValue
  
      if (exitValueIsOk(exitValue)) {
        trace(s"SUCCEEDED: $commandLineString")
      } else {
        trace(s"FAILED: $commandLineString")
      }
      
      JobState.CommandResult(exitValue)
    }.recover {
      case exception: Exception => JobState.FailedWithException(exception)
    }
  }

  override def toString: String = commandLineString
}

object CommandLineJob {

  val mustBeZero: Int => Boolean = _ == 0
  val acceptAll : Int => Boolean = i => true
  
  val defaultExitValueChecker = mustBeZero

  sealed trait CommandResult

  object CommandResult {
    def unapply(cr: CommandResult): Option[(String, Int)] = cr match {
      case CommandSuccess(commandLine, returnValue) => Some(commandLine -> returnValue)
      case CommandFailure(commandLine, returnValue) => Some(commandLine -> returnValue)
      case _ => None
    }
  }
  
  final case class CommandSuccess(commandLine: String, returnValue: Int) extends LJob.Success with CommandResult {
    override def successMessage: String = s"Successfully completed job '$commandLine'."
  }
  
  final case class CommandFailure(commandLine: String, returnValue: Int) extends LJob.Failure with CommandResult {
    override def failureMessage: String = s"Job '$commandLine' completed unsuccessfully with status code $returnValue."
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
