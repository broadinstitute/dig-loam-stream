package loamstream.model.jobs.commandline

import java.nio.file.{Path, Files => JFiles}

import loamstream.model.execute.LocalSettings

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessLogger
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult.{CommandInvocationFailure, CommandResult}
import loamstream.model.jobs.{Execution, JobStatus, LJob, Output}

import scala.util.Success
import scala.util.Failure

/**
  * LoamStream
  * Created by oliverr on 6/17/2016.
  * 
  * A job based on a command line definition 
  */
trait CommandLineJob extends LJob {
  def workDir: Path

  override def workDirOpt: Option[Path] = Some(workDir)

  def processBuilder: ProcessBuilder

  def commandLineString: String

  def logger: ProcessLogger

  def exitValueCheck: Int => Boolean

  def exitValueIsOk(exitValue: Int): Boolean = exitValueCheck(exitValue)

  override def execute(implicit context: ExecutionContext): Future[Execution] = {
    Futures.runBlocking {
      
      val (exitValueAttempt, (start, end)) = TimeUtils.startAndEndTime {
        trace(s"RUNNING: $commandLineString")
        
        createWorkDirAndRun()
      }

      val resources = LocalResources(start, end)
      
      val (jobStatus, jobResult) = exitValueAttempt match {
        case Success(exitValue) => (JobStatus.fromExitCode(exitValue), CommandResult(exitValue))
        case Failure(e) => (JobStatus.FailedWithException, CommandInvocationFailure(e))
      }
      
      Execution(id = None,
                executionEnvironment,
                Some(commandLineString),
                LocalSettings(),
                jobStatus,
                Option(jobResult),
                Option(resources),
                outputs.map(_.toOutputRecord))
    }
  }
  
  private def createWorkDirAndRun(): Int = {
    JFiles.createDirectories(workDir)
      
    val exitValue = processBuilder.run(logger).exitValue

    if (exitValueIsOk(exitValue)) {
      trace(s"SUCCEEDED: $commandLineString")
    } else {
      trace(s"FAILED: $commandLineString")
    }
        
    exitValue
  }

  override def toString: String = s"'$commandLineString'"
}

object CommandLineJob extends Loggable {

  private val mustBeZero: Int => Boolean = _ == 0

  val defaultExitValueChecker = mustBeZero

  val stdErrProcessLogger = ProcessLogger(line => (), line => info(s"(via stderr) $line"))

  def unapply(job: LJob): Option[(String, Set[Output])] = job match {
    case clj: CommandLineJob => Some((clj.commandLineString, clj.outputs))
    case _ => None
  }
}
