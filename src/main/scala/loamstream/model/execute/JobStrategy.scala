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
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.commandline.ProcessLoggers.CloseableProcessLogger
import loamstream.util.BashScript
import loamstream.util.Futures
import loamstream.util.Loggable
import loamstream.util.TimeUtils

/**
 * @author clint
 * Nov 9, 2017
 */
trait JobStrategy {
  //TODO: Type param?
  def execute(implicit context: ExecutionContext): Future[Execution]
}

object JobStrategy {
  def canBeRunLocally(j: LJob): Boolean = j match {
    case _: CommandLineJob | _: NativeJob[_] | _: LocalJob => true
    case _ => false
  }
  
  def localStrategyFor(job: LJob, processLogger: CloseableProcessLogger): JobStrategy = job match {
    case clj: CommandLineJob => CommandLineJobsCanBeRunLocally(clj, processLogger)
    case nj: NativeJob[_] => NativeJobsCanBeRunLocally(nj)
    case lj: LocalJob => LocalJobsCanBeRunLocally(lj)
  }
  
  final case class LocalJobsCanBeRunLocally(lj: LocalJob) extends JobStrategy {
    override def execute(implicit context: ExecutionContext): Future[Execution] = lj.execute
  }
  
  final case class CommandLineJobsCanBeRunLocally(
      commandLineJob: CommandLineJob, 
      processLogger: CloseableProcessLogger) extends JobStrategy with Loggable {
    
    override def execute(implicit context: ExecutionContext): Future[Execution] = {

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

  final case class NativeJobsCanBeRunLocally[A](nativeJob: NativeJob[A]) extends JobStrategy {
    override def execute(implicit context: ExecutionContext): Future[Execution] = {
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
  }
}
