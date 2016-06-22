package loamstream.model.jobs.commandline

import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.commandline.CommandLineJob.{CommandException, CommandNonZeroReturn, CommandResult,
CommandSuccess}

import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.{ProcessBuilder, ProcessLogger}


/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  */
trait CommandLineJob extends LJob {
  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger = CommandLineJob.noOpProcessLogger

  def exitValueCheck: Int => Boolean

  override def execute(implicit context: ExecutionContext): Future[Result with CommandResult] = runBlocking {
    val exitValue = processBuilder.run(logger).exitValue

    if (exitValueCheck(exitValue)) {
      CommandSuccess(commandLineString)
    } else {
      CommandNonZeroReturn(commandLineString, exitValue)
    }
  }.recover {
    case exception: Exception => CommandException(commandLineString, exception)
  }

}

object CommandLineJob {

  val mustBeNonZero: Int => Boolean = _ == 0
  val acceptAll : Int => Boolean = i => true

  sealed trait CommandResult

  final case class CommandSuccess(commandLine: String) extends LJob.Success with CommandResult {
    override def successMessage: String = s"Successfully completed job '$commandLine'."
  }

  final case class CommandException(commandLine: String, exception: Exception)
    extends LJob.Failure with CommandResult {
    override def failureMessage: String = {
      s"Failed with exception '${exception.getMessage}' when trying command line '$commandLine'"
    }
  }

  final case class CommandNonZeroReturn(commandLine: String, returnValue: Int)
    extends LJob.Failure with CommandResult {
    override def failureMessage: String = {
      s"Failed with non-zero return value '$returnValue' when trying command line + '$commandLine'"
    }
  }

  val exitValueSuccess = 0
  val noOpProcessLogger = ProcessLogger(line => ())

}
