package loamstream.model.jobs

import java.nio.file.Path

import _root_.tools.LineCommand.CommandLine
import loamstream.model.jobs.LCommandLineJob.{CommandLineExceptionFailure, CommandLineNonZeroReturnFailure,
CommandLineResult, CommandLineSuccess, exitValueSuccess, noOpProcessLogger}
import loamstream.model.jobs.LJob.Result

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.sys.process.{Process, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
object LCommandLineJob {

  sealed trait CommandLineResult

  case class CommandLineSuccess(commandLine: CommandLine) extends LJob.Success with CommandLineResult {
    override def successMessage: String = "Successfully completed job '" + commandLine.toString + "'"
  }

  case class CommandLineExceptionFailure(commandLine: CommandLine, exception: Exception)
    extends LJob.Failure with CommandLineResult {
    override def failureMessage: String = "Failed with exception '" + exception.getMessage +
      "' when trying command line + '" + commandLine.toString + "'"
  }

  case class CommandLineNonZeroReturnFailure(commandLine: CommandLine, returnValue: Int)
    extends LJob.Failure with CommandLineResult {
    override def failureMessage: String = "Failed with non-zero return value '" + returnValue +
      "' when trying command line + '" + commandLine.toString + "'"
  }

  val exitValueSuccess = 0
  val noOpProcessLogger = ProcessLogger(line => ())
}

case class LCommandLineJob(commandLine: CommandLine, workDir: Path, inputs: Set[LJob],
                           logger: ProcessLogger = noOpProcessLogger)
  extends LJob {

  override def execute(implicit context: ExecutionContext): Future[Result with CommandLineResult] = {
    Future {
      blocking {
        val exitValue = Process(commandLine.tokens, workDir.toFile).run(logger).exitValue
        if (exitValue == exitValueSuccess) {
          CommandLineSuccess(commandLine)
        } else {
          CommandLineNonZeroReturnFailure(commandLine, exitValue)
        }
      }
    }.recover({ case exception: Exception => CommandLineExceptionFailure(commandLine, exception) })
  }
}
