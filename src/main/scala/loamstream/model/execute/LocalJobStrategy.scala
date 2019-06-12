package loamstream.model.execute

import java.nio.file.{Files => JFiles}

import loamstream.model.execute.Resources.LocalResources
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.{JobStatus, LJob, LocalJob, OutputStreams, RunData}
import loamstream.model.jobs.commandline.{CloseableProcessLogger, CommandLineJob, ToFilesProcessLogger}
import loamstream.util.{BashScript, CanBeClosed, Futures, Loggable, TimeUtils}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import java.nio.file.Path

/**
 * @author clint
 * Nov 9, 2017
 */
object LocalJobStrategy extends Loggable {
  def canBeRun(j: LJob): Boolean = j match {
    case _: CommandLineJob | _: LocalJob => true
    case _ => false
  }

  def execute(
    job: LJob,
    jobDir: Path,
    processLogger: ToFilesProcessLogger)(implicit context: ExecutionContext): Future[RunData] = {

    require(canBeRun(job), s"Expected job to be one we can run locally, but got $job")
    
    job match {
      case commandLineJob: CommandLineJob => executeCommandLineJob(commandLineJob, jobDir, processLogger)
      case localJob: LocalJob             => executeLocalJob(localJob)
    }
  }

  private def executeLocalJob(localJob: LocalJob)(implicit ctx: ExecutionContext): Future[RunData] = localJob.execute

  private def executeCommandLineJob(
    commandLineJob: CommandLineJob,
    jobDir: Path,
    processLogger: ToFilesProcessLogger)(implicit context: ExecutionContext): Future[RunData] = {
    
    Futures.runBlocking {
      val (exitValueAttempt, (start, end)) = TimeUtils.startAndEndTime {
        trace(s"RUNNING: ${commandLineJob.commandLineString}")

        CanBeClosed.enclosed(processLogger) {
          createWorkDirAndRun(commandLineJob, _)
        }
      }
      
      val (jobStatus, jobResult) = exitValueAttempt match {
        case Success(exitValue) => (JobStatus.fromExitCode(exitValue), CommandResult(exitValue))
        case Failure(e)         => ExecuterHelpers.statusAndResultFrom(e)
      }
      
      val outputStreams = OutputStreams(processLogger.stdoutPath, processLogger.stderrPath)
      
      RunData(
          job = commandLineJob,
          settings = commandLineJob.initialSettings,
          jobStatus = jobStatus, 
          jobResult = Some(jobResult), 
          resourcesOpt = Some(LocalResources(start, end)), 
          jobDirOpt = Some(jobDir),
          terminationReasonOpt = None)
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
