package loamstream.model.jobs

import java.nio.file.Path

import loamstream.tools.LineCommand.CommandLine
import loamstream.model.jobs.LCommandLineJob.{CommandLineExceptionFailure, CommandLineNonZeroReturnFailure,
CommandLineResult, CommandLineSuccess, exitValueSuccess, noOpProcessLogger}
import loamstream.model.jobs.LJob.Result

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.sys.process.{Process, ProcessLogger}

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final case class LCommandLineJob(
    commandLine: CommandLine, 
    workDir: Path, 
    inputs: Set[LJob],
    logger: ProcessLogger = noOpProcessLogger) extends LJob {

  @deprecated("", "")
  override def toString = s"LCommandLineJob('${commandLine.commandLine}', ...)"
  
  override def withInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs) 
  
  override def execute(implicit context: ExecutionContext): Future[Result with CommandLineResult] = runBlocking {
    val exitValue = Process(commandLine.tokens, workDir.toFile).run(logger).exitValue
    
    if (exitValue == exitValueSuccess) { CommandLineSuccess(commandLine) } 
    else { CommandLineNonZeroReturnFailure(commandLine, exitValue) }
  }.recover { 
    case exception: Exception => CommandLineExceptionFailure(commandLine, exception) 
  }
}

object LCommandLineJob {

  sealed trait CommandLineResult

  final case class CommandLineSuccess(commandLine: CommandLine) extends LJob.Success with CommandLineResult {
    override def successMessage: String = "Successfully completed job '" + commandLine.toString + "'"
  }

  final case class CommandLineExceptionFailure(commandLine: CommandLine, exception: Exception)
    extends LJob.Failure with CommandLineResult {
    override def failureMessage: String = {
      s"Failed with exception '${exception.getMessage}' when trying command line '$commandLine'"
    }
  }

  final case class CommandLineNonZeroReturnFailure(commandLine: CommandLine, returnValue: Int)
    extends LJob.Failure with CommandLineResult {
    override def failureMessage: String = {
      s"Failed with non-zero return value '$returnValue' when trying command line + '$commandLine'"
    }
  }

  val exitValueSuccess = 0
  val noOpProcessLogger = ProcessLogger(line => ())
}
