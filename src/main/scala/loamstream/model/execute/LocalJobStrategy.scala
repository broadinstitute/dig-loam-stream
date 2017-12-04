package loamstream.model.execute

import java.nio.file.{ Files => JFiles }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult
import loamstream.model.jobs.JobResult.CommandInvocationFailure
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LocalJob
import loamstream.model.jobs.NativeJob
import loamstream.model.jobs.commandline.CloseableProcessLogger
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils

/**
 * @author clint
 * Nov 9, 2017
 */
object LocalJobStrategy extends Loggable {
  def canBeRun(j: LJob): Boolean = j match {
    case _: CommandLineJob | _: NativeJob[_] | _: LocalJob => true
    case _ => false
  }

  def execute(
    job: LJob,
    processLogger: CloseableProcessLogger)(implicit context: ExecutionContext): Future[Execution] = {

    require(canBeRun(job), s"Expected job to be one we can run locally, but got $job")
    
    job match {
      case commandLineJob: CommandLineJob => executeCommandLineJob(commandLineJob, processLogger)
      case nativeJob: NativeJob[_]        => executeNativeJob(nativeJob)
      case localJob: LocalJob             => executeLocalJob(localJob)
    }
  }

  def executeLocalJob(localJob: LocalJob)(implicit context: ExecutionContext): Future[Execution] = localJob.execute

  def executeNativeJob[A](nativeJob: NativeJob[A])(implicit context: ExecutionContext): Future[Execution] = {
    val exprBox = nativeJob.exprBox

    exprBox.evalFuture.map { value =>
      Execution(
        id = None,
        env = Environment.Local,
        cmd = None,
        status = JobStatus.Succeeded,
        result = Some(JobResult.ValueSuccess(value, exprBox.typeBox)), // TODO: Is this right?
        resources = None,
        outputs = nativeJob.outputs.map(_.toOutputRecord))
    }
  }

  def executeCommandLineJob(
    commandLineJob: CommandLineJob,
    processLogger: CloseableProcessLogger)(implicit context: ExecutionContext): Future[Execution] = {

    Futures.runBlocking {

      val (exitValueAttempt, (start, end)) = TimeUtils.startAndEndTime {
        trace(s"RUNNING: ${commandLineJob.commandLineString}")

        createWorkDirAndRun(commandLineJob, processLogger)
      }

      val resources = LocalResources(start, end)

      val (jobStatus, jobResult) = exitValueAttempt match {
        case Success(exitValue) => (JobStatus.fromExitCode(exitValue), CommandResult(exitValue))
        case Failure(e)         => (JobStatus.FailedWithException, CommandInvocationFailure(e))
      }

      Execution(
        id = None,
        env = commandLineJob.executionEnvironment,
        cmd = Option(commandLineJob.commandLineString),
        status = jobStatus,
        result = Option(jobResult),
        resources = Option(resources),
        outputs = commandLineJob.outputs.map(_.toOutputRecord))
    }
  }

  private def createWorkDirAndRun(job: CommandLineJob, processLogger: CloseableProcessLogger): Int = {
    JFiles.createDirectories(job.workDir)

    val commandLineString = job.commandLineString

    val processBuilder: scala.sys.process.ProcessBuilder = {
      BashScript.fromCommandLineString(commandLineString).processBuilder(job.workDir)
    }

    val exitValue = processBuilder.run(processLogger).exitValue

    if (job.exitValueIsOk(exitValue)) {
      trace(s"SUCCEEDED: $commandLineString")
    } else {
      trace(s"FAILED: $commandLineString")
    }

    exitValue
  }
}